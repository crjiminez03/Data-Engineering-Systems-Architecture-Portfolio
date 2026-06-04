# CHANGELOG

All notable milestones and changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [3.0.0] — Copilot Studio Agents

### Added
- **Client Resource Agent** — Dual-agent Copilot Studio system for healthcare
  providers. Internal knowledge agent for real-time resource lookup + web-facing
  verification agent to detect and report outdated facility information.
  - Three-layer rule engine (category tags + keyword matching + topic routing)
  - Pay-As-You-Go licensing strategy — full org access without Premium licenses
  - Deployed to Teams, SharePoint, and organizational MS App Store
- **HR Recognition Agent** — Copilot Studio agent processing MS Forms nomination
  submissions via Power Automate. Applies 6-dimension weighted scoring rubric
  to produce objective, evidence-based score cards for recognition council.
  - Non-bias by design — agent scores written evidence only, never identity metadata
  - Council scores independently alongside agent score
  - Automated nominator confirmation and Teams channel notifications

---

## [2.0.0] — Pipeline Orchestration & Gold Layer

### Added
- **Master Pipeline Architecture** — Two-schedule orchestration design:
  - Schedule 1 (6am): Bronze Master → Silver Master (Bronze/Silver layers)
  - Schedule 2 (7am): Gold DDL → Dataflow Gen2 → Gold DML → Semantic Model refresh
- **Bronze Master Pipeline** — Control-table-driven ForEach with Switch activity
  for multi-server routing (SERVER1, SERVER2) and If Condition for Full vs
  Incremental load branching. Complete Fabric Pipeline expression reference.
- **Gold DDL Notebook** — Precision-preserving table definitions using explicit
  DECIMAL columns to prevent Dataflow Gen2 Replace method schema downcasting.
- **Gold DML Notebook** — Staging → Gold MERGE with hash-based change detection,
  `_dw_created_at` preservation, and post-merge row count validation.
- **Dataflow Gen2 M Queries** — Three Power Query M transformations for Gold layer:
  refresh summary, audit user activity, model health scorecard with A/B/C/D grading.
- **Pipeline Audit Log** — Full DDL for `pipeline_audit_log` table + `audit_sp()`
  Python function callable from any notebook in the medallion pipeline.
- **Control Table** — DDL + seed data for `control_table` in Silver Warehouse
  driving all pipeline refresh decisions (watermark, load type, server switch).
- **Silver Master Notebook** — Orchestrator calling all Silver DML notebooks via
  `mssparkutils.notebook.run()` with parameter passing and watermark advancement.

### Fixed
- Corrected SemanticModelLogs architecture — removed incorrect Bronze/Silver layers.
  SemanticModelLogs is Gold-layer only (logs activity ON TOP of Gold tables).

---

## [1.1.0] — Metadata Master & Full Lineage

### Added
- **Lakehouse System Tables Catalog** — Iterates all 3 Lakehouse layers via
  `spark.catalog.listTables()` and Delta history to build physical column inventory.
- **Metadata Master Full Lineage** — Two-source join combining:
  - SOURCE 1: Semantic model objects from live PBIX XMLA connection (sempy)
  - SOURCE 2: Lakehouse physical columns from system tables
  - Lineage map connecting semantic model tables to Gold/Silver Delta tables
  - Type mismatch detection, orphaned column flagging, storage size enrichment
- **Column Profiling v2** — Metadata-catalog-driven profiling using
  `metadata_master_full` as the catalog of record. Carries full semantic
  lineage on every result row — identifies which reports are impacted by
  a data quality issue.
- **Semantic Model System Tables (Live)** — Replaced CSV export approach with
  live PBIX connection via `sempy.fabric.evaluate_dax()` querying
  `$SYSTEM.TMSCHEMA_*` and `$SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS` directly.

---

## [1.0.0] — Medallion Architecture Foundation

### Added
- **Bronze Layer** — KQL → Delta ingestion for SemanticModelLogs and
  O365 Management API audit log ingestion with full blob pagination
  (bypasses 5,000-row PowerShell limit). Runs at 2am daily for prior day.
- **Silver Layer** — DDL + MERGE upsert for both SemanticModelLogs and
  audit logs. Full JSON parsing of Identity, ApplicationContext, EventText.
  Row hash for change detection. Incremental watermark-driven loads.
- **Gold Layer SemanticModelLogs** — KQL ingestion + parsed table with
  UPN, ReportId, VisualId, PageId, VisualName, PageName extracted from
  ApplicationContext Sources array and DAX query text from EventText.
- **Gold Analytics** — Report/visual usage, DAX query performance tiers,
  user engagement classification, daily model usage summary.
- **KQL Queries** — 20 total KQL queries across workspace logs and
  SemanticModelLogs: refresh health, slow DAX, CPU/IO profiling,
  duplicate detection, real-time feed, watermark check.
- **Row Count Validation** — Pre/post ingestion comparison with
  CRITICAL/WARN/PASS classification. Silent failure detection cross-
  referencing SUCCESS pipeline status against actual loaded row counts.
- **Metadata Health Checks** — Null rates, column usage, measure catalog,
  row count summary across Bronze/Silver/Gold.
- **Column Profiling** — Statistical profiling: min/max/avg/stddev/
  percentiles for numeric columns; length stats for strings; cardinality
  analysis; health flags (CRITICAL/WARN/OK).
- **SQL Server Job Monitor** — 3-section T-SQL query against msdb + DMVs:
  currently running jobs with overrun % vs historical average, completed
  anomaly detection, full job health scorecard with coefficient of variation.

---

## [0.1.0] — Project Initialisation

### Added
- Initial repository structure established
- README.md with project overview and architecture diagram
- Folder structure: kql/, semantic_model/, audit_logs/, metadata/,
  validation/, pipeline_docs/, copilot_agents/, sql_server_ops/,
  reporting_layer/


---

## [3.1.0] — CI/CD & DevOps Integration

### Added
- **Fabric Deployment Pipeline** — Two-stage Dev → Prod promotion pipeline
  with pre-promotion checklist, diff review gate, and post-promotion
  validation steps.
- **Azure DevOps Git Integration** — Production workspace connected to
  Azure DevOps main branch. All notebooks, Data Pipeline JSON definitions,
  Dataflow Gen2 M queries (.pq), semantic model (.bim with DAX measures),
  and Spark Environment definitions version-controlled automatically on
  every promotion sync.
- **Rollback process** — Documented DevOps revert → Fabric sync rollback
  path for any promoted item.
- **Deployment architecture document** — Full environment strategy diagram,
  repo structure, promotion steps, branch strategy, and setup reference.
