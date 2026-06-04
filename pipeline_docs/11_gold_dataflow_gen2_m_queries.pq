// ============================================================
// Gold Layer — Dataflow Gen2 M Query Examples
// Portfolio Reference: Fabric Workspace Observability Project
// Description: Power Query M transformations used in Dataflow Gen2
//              to produce gold-layer tables from Silver Delta tables.
//              Each section is a separate Dataflow query/table.
// ============================================================


// ─────────────────────────────────────────────────────────────
// Gold Table 1: Daily Refresh Health Summary
// Source: silver_semantic_model_logs_parsed (Silver Lakehouse)
// ─────────────────────────────────────────────────────────────
let
    Source = Lakehouse.Contents(null){[workspaceId="YOUR_WORKSPACE_GUID",lakehouseId="YOUR_LAKEHOUSE_GUID"]}[Data],
    Silver = Source{[Id="silver_semantic_model_logs_parsed",Kind="Table"]}[Data],

    // Filter to refresh operations only
    FilterRefresh = Table.SelectRows(Silver, each Text.Contains([OperationName], "Refresh")),

    // Cast types
    CastTypes = Table.TransformColumnTypes(FilterRefresh, {
        {"Timestamp",   type datetime},
        {"DurationMs",  Int64.Type},
        {"CpuTimeMs",   Int64.Type}
    }),

    // Add log_date column
    AddLogDate = Table.AddColumn(CastTypes, "log_date", each Date.From([Timestamp]), type date),

    // Group by date + model
    Grouped = Table.Group(AddLogDate,
        {"log_date", "WorkspaceName", "ItemName", "ModelMode"},
        {
            {"total_refreshes",  each Table.RowCount(_),                       Int64.Type},
            {"success_count",    each List.Count(List.Select([Status], each _ = "SUCCESS")), Int64.Type},
            {"failure_count",    each List.Count(List.Select([Status], each _ = "FAILURE")), Int64.Type},
            {"avg_duration_ms",  each List.Average([DurationMs]),               type number},
            {"max_duration_ms",  each List.Max([DurationMs]),                   Int64.Type},
            {"min_duration_ms",  each List.Min([DurationMs]),                   Int64.Type},
            {"avg_cpu_ms",       each List.Average([CpuTimeMs]),                type number}
        }
    ),

    // Add success rate
    AddSuccessRate = Table.AddColumn(Grouped, "success_rate_pct",
        each if [total_refreshes] > 0
             then Number.Round([success_count] / [total_refreshes] * 100, 2)
             else null,
        type number),

    // Add SLA status flag (> 30 mins avg = breach)
    AddSLA = Table.AddColumn(AddSuccessRate, "sla_status",
        each if [avg_duration_ms] > 1800000 then "SLA_BREACH" else "OK",
        type text),

    // Add load timestamp
    AddLoadTs = Table.AddColumn(AddSLA, "gold_loaded_at",
        each DateTimeZone.UtcNow(), type datetimezone),

    // Sort
    Sorted = Table.Sort(AddLoadTs, {{"log_date", Order.Descending}})
in
    Sorted


// ─────────────────────────────────────────────────────────────
// Gold Table 2: Audit Log User Activity Summary
// Source: silver_audit_logs_powerbi (Silver Lakehouse)
// ─────────────────────────────────────────────────────────────
let
    Source = Lakehouse.Contents(null){[workspaceId="YOUR_WORKSPACE_GUID",lakehouseId="YOUR_LAKEHOUSE_GUID"]}[Data],
    SilverAudit = Source{[Id="silver_audit_logs_powerbi",Kind="Table"]}[Data],

    // Cast
    CastTypes = Table.TransformColumnTypes(SilverAudit, {
        {"CreationTime", type datetime},
        {"IsSuccess",    type logical}
    }),

    // Add date column
    AddDate = Table.AddColumn(CastTypes, "log_date", each Date.From([CreationTime]), type date),

    // Group by user + operation + day
    Grouped = Table.Group(AddDate,
        {"log_date", "UserId", "Operation", "WorkSpaceName"},
        {
            {"total_events",   each Table.RowCount(_),                             Int64.Type},
            {"success_events", each List.Count(List.Select([IsSuccess], each _ = true)),  Int64.Type},
            {"failure_events", each List.Count(List.Select([IsSuccess], each _ = false)), Int64.Type},
            {"unique_items",   each List.Count(List.Distinct([ItemName])),           Int64.Type}
        }
    ),

    // Success rate
    AddRate = Table.AddColumn(Grouped, "success_rate_pct",
        each if [total_events] > 0
             then Number.Round([success_events] / [total_events] * 100, 2)
             else null,
        type number),

    // Classify user type by event volume
    ClassifyUser = Table.AddColumn(AddRate, "user_tier",
        each if [total_events] >= 1000 then "Heavy"
             else if [total_events] >= 100 then "Moderate"
             else "Light",
        type text),

    AddLoadTs = Table.AddColumn(ClassifyUser, "gold_loaded_at",
        each DateTimeZone.UtcNow(), type datetimezone),

    Sorted = Table.Sort(AddLoadTs, {{"log_date", Order.Descending}, {"total_events", Order.Descending}})
in
    Sorted


// ─────────────────────────────────────────────────────────────
// Gold Table 3: Model Health Scorecard
// Source: gold_semantic_model_refresh_summary (Gold Lakehouse)
//         + gold_semantic_model_dax_performance
// ─────────────────────────────────────────────────────────────
let
    Source = Lakehouse.Contents(null){[workspaceId="YOUR_WORKSPACE_GUID",lakehouseId="YOUR_LAKEHOUSE_GUID"]}[Data],

    // Read both gold tables
    RefreshSummary = Source{[Id="gold_semantic_model_refresh_summary",Kind="Table"]}[Data],
    DaxPerf        = Source{[Id="gold_semantic_model_dax_performance",Kind="Table"]}[Data],

    // Aggregate refresh summary per model (all time recent 30d)
    RefreshAgg = Table.Group(RefreshSummary,
        {"ItemName", "WorkspaceName", "ModelMode"},
        {
            {"avg_refresh_success_rate", each List.Average([success_rate_pct]),   type number},
            {"avg_refresh_duration_ms",  each List.Average([avg_duration_ms]),    type number},
            {"sla_breach_days",          each List.Count(List.Select([sla_status], each _ = "SLA_BREACH")), Int64.Type},
            {"total_refresh_days",       each Table.RowCount(_),                  Int64.Type}
        }
    ),

    // Aggregate DAX perf per model
    DaxAgg = Table.Group(DaxPerf,
        {"ItemName", "WorkspaceName"},
        {
            {"avg_dax_duration_ms",   each List.Average([avg_duration_ms]),  type number},
            {"max_dax_duration_ms",   each List.Max([max_duration_ms]),      Int64.Type},
            {"total_dax_executions",  each List.Sum([dax_execution_count]),  Int64.Type},
            {"pct_slow_queries",
             each
                 let slow = List.Count(List.Select([performance_tier], each _ = "Slow (2-10s)" or _ = "Critical (>10s)")),
                     total = Table.RowCount(_)
                 in if total > 0 then Number.Round(slow / total * 100, 2) else 0,
             type number
            }
        }
    ),

    // Join on ItemName + WorkspaceName
    Joined = Table.NestedJoin(RefreshAgg, {"ItemName","WorkspaceName"},
                               DaxAgg,    {"ItemName","WorkspaceName"},
                               "DaxMetrics", JoinKind.Left),
    Expanded = Table.ExpandTableColumn(Joined, "DaxMetrics",
        {"avg_dax_duration_ms","max_dax_duration_ms","total_dax_executions","pct_slow_queries"}),

    // Overall health score (simple weighted: 50% refresh, 30% DAX speed, 20% SLA)
    AddHealthScore = Table.AddColumn(Expanded, "health_score",
        each
            let
                refreshScore = Number.Min([avg_refresh_success_rate] ?? 0, 100) * 0.5,
                daxScore     = (100 - Number.Min([pct_slow_queries] ?? 0, 100)) * 0.3,
                slaScore     = (1 - Number.Min([sla_breach_days] ?? 0, [total_refresh_days]) /
                                    Number.Max([total_refresh_days], 1)) * 100 * 0.2
            in
                Number.Round(refreshScore + daxScore + slaScore, 1),
        type number),

    AddGrade = Table.AddColumn(AddHealthScore, "health_grade",
        each if [health_score] >= 90 then "A"
             else if [health_score] >= 75 then "B"
             else if [health_score] >= 60 then "C"
             else "D",
        type text),

    AddLoadTs = Table.AddColumn(AddGrade, "gold_loaded_at",
        each DateTimeZone.UtcNow(), type datetimezone),

    Sorted = Table.Sort(AddLoadTs, {{"health_score", Order.Ascending}})
in
    Sorted
