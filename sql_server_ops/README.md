# /sql_server_ops — SQL Server Operations & Monitoring

## Overview

T-SQL queries for SQL Server operational monitoring, specifically focused
on SQL Agent job health analysis. This work was part of a broader
on-premises SQL Server operations role that preceded the Microsoft Fabric
migration project — demonstrating SQL Server DBA-adjacent skills alongside
the modern Fabric data engineering work.

## Files

| File | Purpose |
|------|---------|
| `12_long_running_jobs_monitor.sql` | See pipeline_docs/ — referenced here for context |

## Long Running Jobs Monitor — What It Does

A single SQL file containing three complementary queries against `msdb`
system tables and SQL Server DMVs:

### Section 1: Currently Running Jobs (with Overrun %)
The core query. Joins:
- `msdb.dbo.sysjobactivity` — which jobs are currently executing
- `msdb.dbo.sysjobhistory` — historical run records (last 30 runs per job)
- `sys.dm_exec_requests` — active request details from DMV
- `sys.dm_exec_sessions` — session context
- `sys.dm_exec_sql_text` — actual SQL text of running step

Calculates per currently-running job:
- `current_elapsed_seconds` — how long it has been running
- `hist_avg_duration_sec` — rolling average of last 30 successful runs
- `overrun_seconds` — how many seconds over the average it currently is
- `overrun_pct` — percentage over/under historical average
- `est_completion_time` — estimated finish based on historical average
- `overrun_status` — NORMAL / WARNING (>20% over) / CRITICAL (>50% over)

### Section 2: Recently Completed Anomalies
Shows jobs that finished in the last lookback window but ran significantly
longer than their historical average. Catches post-run anomalies that
wouldn't appear in the currently-running query.

### Section 3: Job Health Scorecard
Full summary of all jobs with: failure rate %, average/max/min duration,
standard deviation, coefficient of variation (stddev/avg — high CV = erratic
run times), and last run status. Useful for identifying chronically unreliable
jobs before they become incidents.

## Configuration Parameters (top of file)

```sql
DECLARE @HistoryLookbackDays    INT = 90;   -- History window for avg calculation
DECLARE @MinHistoryRuns         INT = 3;    -- Min runs needed to compute overrun %
DECLARE @OverrunWarningPct      INT = 20;   -- Warning threshold
DECLARE @OverrunCriticalPct     INT = 50;   -- Critical threshold
DECLARE @MaxHistoryRunsForAvg   INT = 30;   -- Rolling window for average
```

## Required Permissions

```sql
-- For DMV access (sys.dm_exec_requests etc.)
GRANT VIEW SERVER STATE TO [your_login];

-- For msdb job history
-- Requires membership in: SQLAgentReaderRole (msdb) or sysadmin
```

## Practical Use Cases

- **Morning ops check:** Run Section 1 at start of day to see if any
  overnight jobs are still running and how far over schedule they are
- **Incident response:** When a job is reported slow, Section 1 shows
  exact overrun % and whether it is CPU-bound or IO-bound (via DMV wait type)
- **Capacity planning:** Section 3 scorecard identifies jobs with high
  coefficient of variation — these are candidates for optimization or
  schedule adjustment before they cause downstream issues
- **SLA reporting:** Historical averages and max durations from Section 3
  can feed into SLA breach reporting for business stakeholders
