/*
================================================================================
  SQL Server — Long Running Jobs Monitor
  Portfolio Reference: SQL Server Operations & Monitoring Project
  Author: [Your Name]

  Description:
    Identifies currently executing SQL Agent jobs and calculates how long each
    job has been running vs its historical average duration (from msdb job history).
    Computes an overrun percentage so you can immediately see which jobs are
    running significantly longer than expected — even if they haven't failed yet.

    Also surfaces recently completed jobs that ran longer than their historical
    average as a post-run anomaly report.

  Tables Used:
    msdb.dbo.sysjobs              — Job definitions
    msdb.dbo.sysjobsteps          — Job step definitions
    msdb.dbo.sysjobhistory        — Historical run records
    msdb.dbo.sysjobactivity       — Currently running job sessions
    sys.dm_exec_requests          — Active request details (CPU, reads, blocking)
    sys.dm_exec_sessions          — Session details
    sys.dm_exec_sql_text          — SQL text of active requests
    sys.dm_os_wait_stats          — Wait type context (joined via session)

  Notes:
    - Requires VIEW SERVER STATE permission for DMV access
    - msdb access requires SQLAgentReaderRole or sysadmin
    - Duration stored in sysjobhistory as HHMMSS integer — converted to seconds below
    - Historical average uses last 30 completed runs per job (configurable)
    - Minimum 3 historical runs required before overrun % is calculated
================================================================================
*/


-- ============================================================
-- SECTION 1: Configuration
-- ============================================================
DECLARE @HistoryLookbackDays    INT = 90;    -- How far back to pull history for avg calc
DECLARE @MinHistoryRuns         INT = 3;     -- Min completed runs needed to compute avg
DECLARE @OverrunWarningPct      INT = 20;    -- Flag if running > 20% over avg
DECLARE @OverrunCriticalPct     INT = 50;    -- Flag if running > 50% over avg
DECLARE @MaxHistoryRunsForAvg   INT = 30;    -- Use only the last N runs for rolling avg


-- ============================================================
-- SECTION 2: Parse sysjobhistory duration (HHMMSS → seconds)
-- ============================================================
/*
  msdb stores run_duration as an integer in HHMMSS format e.g. 10523 = 1h 5m 23s
  This CTE normalises it to total seconds for arithmetic.
*/
;WITH

HistoryParsed AS (
    SELECT
        jh.job_id,
        jh.step_id,
        jh.step_name,
        jh.run_status,                          -- 0=Fail 1=Success 2=Retry 3=Cancel 4=InProgress
        -- Convert HHMMSS integer to total seconds
        ( (jh.run_duration / 10000)       * 3600   -- hours
        + ((jh.run_duration % 10000) / 100) * 60   -- minutes
        +  (jh.run_duration % 100)                 -- seconds
        )                                   AS run_duration_seconds,
        -- Reconstruct run start datetime from msdb's split date/time integers
        DATEADD(
            SECOND,
            ( (jh.run_time / 10000)       * 3600
            + ((jh.run_time % 10000) / 100) * 60
            +  (jh.run_time % 100)
            ),
            CAST(
                CAST(jh.run_date AS CHAR(8)) AS DATE  -- yyyyMMdd → date
            )
        )                                   AS run_start_datetime,
        jh.message                          AS outcome_message,
        ROW_NUMBER() OVER (
            PARTITION BY jh.job_id, jh.step_id
            ORDER BY jh.run_date DESC, jh.run_time DESC
        )                                   AS run_recency_rank
    FROM msdb.dbo.sysjobhistory jh
    WHERE
        jh.run_status IN (0, 1, 3)              -- Exclude InProgress rows (4) from history
        AND jh.step_id <> 0                     -- Exclude job-level summary rows (step 0 is the rollup)
        AND DATEADD(
                SECOND,
                ( (jh.run_time / 10000) * 3600
                + ((jh.run_time % 10000) / 100) * 60
                + (jh.run_time % 100)),
                CAST(CAST(jh.run_date AS CHAR(8)) AS DATE)
            ) >= DATEADD(DAY, -@HistoryLookbackDays, GETDATE())
),

-- ============================================================
-- SECTION 3: Historical averages per job + step
--            (rolling last N successful runs only)
-- ============================================================
HistoryAvg AS (
    SELECT
        job_id,
        step_id,
        COUNT(*)                                            AS total_historical_runs,
        SUM(CASE WHEN run_status = 1 THEN 1 ELSE 0 END)    AS successful_runs,
        SUM(CASE WHEN run_status = 0 THEN 1 ELSE 0 END)    AS failed_runs,
        AVG(CASE WHEN run_status = 1 AND run_recency_rank <= @MaxHistoryRunsForAvg
                 THEN run_duration_seconds END)             AS avg_duration_seconds_success,
        MAX(CASE WHEN run_status = 1 THEN run_duration_seconds END) AS max_duration_seconds,
        MIN(CASE WHEN run_status = 1 THEN run_duration_seconds END) AS min_duration_seconds,
        -- Stddev to detect unusually variable jobs
        STDEV(CASE WHEN run_status = 1 AND run_recency_rank <= @MaxHistoryRunsForAvg
                   THEN CAST(run_duration_seconds AS FLOAT) END) AS stddev_duration_seconds,
        -- Last run metadata
        MAX(run_start_datetime)                             AS last_run_datetime,
        MAX(CASE WHEN run_recency_rank = 1 THEN run_duration_seconds END) AS last_run_duration_seconds,
        MAX(CASE WHEN run_recency_rank = 1 THEN run_status END)           AS last_run_status
    FROM HistoryParsed
    GROUP BY job_id, step_id
),

-- ============================================================
-- SECTION 4: Currently running jobs
--            Joined to SQL Agent activity + DMVs
-- ============================================================
CurrentlyRunning AS (
    SELECT
        ja.job_id,
        ja.start_execution_date                             AS job_start_time,
        DATEDIFF(SECOND, ja.start_execution_date, GETDATE()) AS current_elapsed_seconds,
        ja.last_executed_step_id                            AS current_step_id,
        ja.last_executed_step_date                          AS step_start_time,
        DATEDIFF(SECOND, ja.last_executed_step_date, GETDATE()) AS step_elapsed_seconds,
        -- Link to DMVs via session SPID stored in job activity
        sj.session_id                                       AS agent_session_id
    FROM msdb.dbo.sysjobactivity ja
    -- sysjobactivity has one row per job per agent session; filter to current session
    INNER JOIN (
        SELECT MAX(session_id) AS session_id
        FROM msdb.dbo.syssessions
    ) sj ON ja.session_id = sj.session_id
    WHERE
        ja.start_execution_date IS NOT NULL
        AND ja.stop_execution_date IS NULL      -- Still running
),

-- ============================================================
-- SECTION 5: DMV — Active requests for running job processes
-- ============================================================
ActiveRequests AS (
    SELECT
        es.login_name,
        er.session_id,
        er.status                                           AS request_status,
        er.wait_type,
        er.wait_time                                        AS wait_time_ms,
        er.blocking_session_id,
        er.cpu_time                                         AS cpu_time_ms,
        er.logical_reads,
        er.writes,
        er.total_elapsed_time                               AS elapsed_time_ms,
        er.percent_complete,
        er.command,
        st.text                                             AS sql_text,
        er.plan_handle
    FROM sys.dm_exec_requests er
    INNER JOIN sys.dm_exec_sessions es
        ON er.session_id = es.session_id
    CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) st
    WHERE
        es.is_user_process = 1
        AND er.session_id <> @@SPID              -- Exclude this query's own session
)


-- ============================================================
-- SECTION 6: MAIN OUTPUT — Currently Running Jobs
--            with Overrun % vs Historical Average
-- ============================================================
SELECT
    -- ── Job Identity ──────────────────────────────────────────
    j.name                                                  AS job_name,
    js.step_name                                            AS current_step_name,
    cr.current_step_id,
    j.description                                           AS job_description,
    j.enabled                                               AS job_is_enabled,

    -- ── Timing ────────────────────────────────────────────────
    cr.job_start_time,
    cr.step_start_time,
    cr.current_elapsed_seconds                              AS job_total_elapsed_sec,
    cr.step_elapsed_seconds                                 AS step_elapsed_sec,

    -- Format elapsed as HH:MM:SS for readability
    CONVERT(VARCHAR(8),
        DATEADD(SECOND, cr.current_elapsed_seconds, 0), 108
    )                                                       AS job_elapsed_hhmm_ss,

    -- ── Historical Benchmarks ─────────────────────────────────
    ha.avg_duration_seconds_success                         AS hist_avg_duration_sec,
    ha.max_duration_seconds                                 AS hist_max_duration_sec,
    ha.min_duration_seconds                                 AS hist_min_duration_sec,
    ha.stddev_duration_seconds                              AS hist_stddev_sec,
    ha.total_historical_runs,
    ha.successful_runs                                      AS hist_successful_runs,
    ha.failed_runs                                          AS hist_failed_runs,

    CONVERT(VARCHAR(8),
        DATEADD(SECOND, ISNULL(ha.avg_duration_seconds_success, 0), 0), 108
    )                                                       AS hist_avg_hhmm_ss,

    -- ── Overrun Calculation ────────────────────────────────────
    CASE
        WHEN ha.avg_duration_seconds_success IS NULL
          OR ha.successful_runs < @MinHistoryRuns
        THEN NULL
        ELSE cr.current_elapsed_seconds - ha.avg_duration_seconds_success
    END                                                     AS overrun_seconds,

    -- % over or under historical average (negative = running faster than avg)
    CASE
        WHEN ha.avg_duration_seconds_success IS NULL
          OR ha.avg_duration_seconds_success = 0
          OR ha.successful_runs < @MinHistoryRuns
        THEN NULL
        ELSE CAST(
            ROUND(
                100.0 * (cr.current_elapsed_seconds - ha.avg_duration_seconds_success)
                      / ha.avg_duration_seconds_success
                , 1
            ) AS DECIMAL(10,1)
        )
    END                                                     AS overrun_pct,

    -- ── Alert Classification ──────────────────────────────────
    CASE
        WHEN ha.successful_runs < @MinHistoryRuns
            THEN 'INSUFFICIENT_HISTORY'
        WHEN ha.avg_duration_seconds_success IS NULL
            THEN 'NO_SUCCESS_HISTORY'
        WHEN cr.current_elapsed_seconds > ha.avg_duration_seconds_success * (1 + @OverrunCriticalPct / 100.0)
            THEN 'CRITICAL'
        WHEN cr.current_elapsed_seconds > ha.avg_duration_seconds_success * (1 + @OverrunWarningPct / 100.0)
            THEN 'WARNING'
        WHEN cr.current_elapsed_seconds < ha.avg_duration_seconds_success
            THEN 'RUNNING_FAST'
        ELSE 'NORMAL'
    END                                                     AS overrun_status,

    -- Estimated completion (based on historical avg — null if insufficient history)
    CASE
        WHEN ha.avg_duration_seconds_success > 0
          AND ha.successful_runs >= @MinHistoryRuns
        THEN DATEADD(SECOND,
                ha.avg_duration_seconds_success,
                cr.job_start_time)
        ELSE NULL
    END                                                     AS est_completion_time,

    -- Is it past the estimated completion already?
    CASE
        WHEN ha.avg_duration_seconds_success > 0
          AND ha.successful_runs >= @MinHistoryRuns
          AND GETDATE() > DATEADD(SECOND, ha.avg_duration_seconds_success, cr.job_start_time)
        THEN 'YES'
        ELSE 'NO'
    END                                                     AS past_estimated_completion,

    -- ── DMV Details (if process is traceable) ─────────────────
    ar.request_status                                       AS spid_status,
    ar.wait_type,
    ar.wait_time_ms,
    ar.blocking_session_id,
    ar.cpu_time_ms,
    ar.logical_reads,
    ar.writes,
    ar.percent_complete                                     AS pct_complete_dmv,
    -- percent_complete is non-zero only for specific operations:
    -- BACKUP, RESTORE, DBCC CHECKDB, ALTER INDEX REBUILD, etc.
    CASE
        WHEN ar.percent_complete > 0
        THEN CAST(ROUND(ar.percent_complete, 1) AS VARCHAR) + '%'
        ELSE 'N/A (not trackable via DMV)'
    END                                                     AS dmv_pct_complete_display,
    ar.sql_text,
    ha.last_run_datetime,
    ha.last_run_duration_seconds

FROM CurrentlyRunning cr
INNER JOIN msdb.dbo.sysjobs j
    ON cr.job_id = j.job_id
LEFT JOIN msdb.dbo.sysjobsteps js
    ON cr.job_id = js.job_id
    AND cr.current_step_id = js.step_id
LEFT JOIN HistoryAvg ha
    ON cr.job_id = ha.job_id
    AND cr.current_step_id = ha.step_id
LEFT JOIN ActiveRequests ar
    ON ar.session_id = cr.agent_session_id

ORDER BY
    -- Sort by most critical first, then by most overrun
    CASE
        WHEN CASE
                WHEN ha.avg_duration_seconds_success > 0 AND ha.successful_runs >= @MinHistoryRuns
                THEN CAST(ROUND(100.0 * (cr.current_elapsed_seconds - ha.avg_duration_seconds_success)
                              / ha.avg_duration_seconds_success, 1) AS DECIMAL(10,1))
                ELSE NULL
             END > @OverrunCriticalPct THEN 1
        WHEN CASE
                WHEN ha.avg_duration_seconds_success > 0 AND ha.successful_runs >= @MinHistoryRuns
                THEN CAST(ROUND(100.0 * (cr.current_elapsed_seconds - ha.avg_duration_seconds_success)
                              / ha.avg_duration_seconds_success, 1) AS DECIMAL(10,1))
                ELSE NULL
             END > @OverrunWarningPct  THEN 2
        ELSE 3
    END,
    cr.current_elapsed_seconds DESC;


-- ============================================================
-- SECTION 7: COMPLETED JOBS — Recent Anomalies
--            Jobs that finished but ran > X% over their avg
-- ============================================================
SELECT
    j.name                                                  AS job_name,
    js.step_name,
    hp.run_start_datetime,
    hp.run_duration_seconds                                 AS actual_duration_sec,
    CONVERT(VARCHAR(8),
        DATEADD(SECOND, hp.run_duration_seconds, 0), 108)  AS actual_hhmm_ss,
    ha.avg_duration_seconds_success                         AS hist_avg_duration_sec,
    CONVERT(VARCHAR(8),
        DATEADD(SECOND, ISNULL(ha.avg_duration_seconds_success,0), 0), 108) AS hist_avg_hhmm_ss,
    hp.run_duration_seconds - ha.avg_duration_seconds_success AS overrun_seconds,
    CAST(
        ROUND(
            100.0 * (hp.run_duration_seconds - ha.avg_duration_seconds_success)
                  / NULLIF(ha.avg_duration_seconds_success, 0)
            , 1
        ) AS DECIMAL(10,1)
    )                                                       AS overrun_pct,
    CASE
        WHEN hp.run_status = 1 THEN 'Success'
        WHEN hp.run_status = 0 THEN 'Failed'
        WHEN hp.run_status = 3 THEN 'Cancelled'
        ELSE 'Other'
    END                                                     AS run_outcome,
    ha.total_historical_runs,
    ha.failed_runs                                          AS hist_failed_runs,
    hp.outcome_message

FROM HistoryParsed hp
INNER JOIN HistoryAvg ha
    ON hp.job_id = ha.job_id
    AND hp.step_id = ha.step_id
INNER JOIN msdb.dbo.sysjobs j
    ON hp.job_id = j.job_id
LEFT JOIN msdb.dbo.sysjobsteps js
    ON hp.job_id = js.job_id
    AND hp.step_id = js.step_id
WHERE
    hp.run_recency_rank <= 10                              -- Last 10 runs per step
    AND ha.successful_runs >= @MinHistoryRuns              -- Need enough history
    AND ha.avg_duration_seconds_success > 0
    AND hp.run_duration_seconds >
        ha.avg_duration_seconds_success * (1 + @OverrunWarningPct / 100.0)  -- Only anomalies

ORDER BY
    overrun_pct DESC,
    hp.run_start_datetime DESC;


-- ============================================================
-- SECTION 8: JOB HEALTH SUMMARY — All jobs, last 30 days
--            Includes avg duration, failure rate, trend signal
-- ============================================================
SELECT
    j.name                                                  AS job_name,
    j.enabled,
    ha.step_id,
    js.step_name,
    ha.total_historical_runs,
    ha.successful_runs,
    ha.failed_runs,
    CAST(
        ROUND(100.0 * ha.failed_runs / NULLIF(ha.total_historical_runs, 0), 1)
    AS DECIMAL(5,1))                                        AS failure_rate_pct,
    ha.avg_duration_seconds_success                         AS avg_duration_sec,
    ha.max_duration_seconds                                 AS max_duration_sec,
    ha.min_duration_seconds                                 AS min_duration_sec,
    CAST(ROUND(ha.stddev_duration_seconds, 1) AS DECIMAL(10,1)) AS stddev_sec,
    -- Coefficient of variation: stddev/avg — high = erratic run times
    CASE
        WHEN ha.avg_duration_seconds_success > 0
        THEN CAST(ROUND(ha.stddev_duration_seconds / ha.avg_duration_seconds_success, 3) AS DECIMAL(6,3))
        ELSE NULL
    END                                                     AS coeff_of_variation,
    CASE
        WHEN ha.stddev_duration_seconds / NULLIF(ha.avg_duration_seconds_success,0) > 0.5
        THEN 'HIGH_VARIANCE'
        ELSE 'STABLE'
    END                                                     AS run_time_stability,
    CONVERT(VARCHAR(8),
        DATEADD(SECOND, ISNULL(ha.avg_duration_seconds_success,0), 0), 108) AS avg_hhmm_ss,
    ha.last_run_datetime,
    CASE ha.last_run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Success'
        WHEN 3 THEN 'Cancelled'
        ELSE 'Unknown'
    END                                                     AS last_run_status

FROM HistoryAvg ha
INNER JOIN msdb.dbo.sysjobs j
    ON ha.job_id = j.job_id
LEFT JOIN msdb.dbo.sysjobsteps js
    ON ha.job_id = js.job_id
    AND ha.step_id = js.step_id
ORDER BY
    failure_rate_pct DESC,
    ha.avg_duration_seconds_success DESC;
