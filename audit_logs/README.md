# /audit_logs — Power BI Audit Log Pipeline (O365 Management API)

## Overview

Ingests Power BI audit events (RecordType = 21) from the Office 365
Management Activity API for a full 24-hour window daily. Designed to
run at **2:00 AM UTC** to capture the previous day's complete audit data.

## The Problem This Solves

The previous PowerShell-based audit log export using `Search-UnifiedAuditLog`
hit a hard **5,000-row cap** per API call with no reliable pagination.
High-activity days silently lost data beyond that threshold with no error.

## The Solution

The O365 Management Activity API returns paginated **content blob URIs**
via a `NextPageUri` response header. Each blob contains up to 1,000 records.
By iterating all blobs across all pages, every audit record for the full
24-hour window is captured regardless of volume.

```
Management API
    │
    ├── GET /subscriptions/content → Page 1 blob URIs
    │         NextPageUri header ──→ Page 2 blob URIs
    │                                      │
    │                               Page N blob URIs
    │
    └── For each URI → GET blob → filter RecordType=21
                                → append to collection
                                → total: ALL records, no cap
```

## Files

| File | Layer | Purpose |
|------|-------|---------|
| `07_bronze_audit_logs_graph_api.py` | Bronze | Full pagination audit log ingestion. Authenticates via service principal (Key Vault). Ensures subscription is active. Iterates all content blob URIs. Flattens records to Delta table partitioned by `_batch_date`. |
| `08_silver_audit_logs_ddl_dml.py` | Silver | DDL table creation with full column definitions. Incremental watermark-driven load. Parses `IsSuccess` to boolean, extracts `AppId`/`CapacityId`/`ArtifactKind` from `raw_json`. MERGE upsert on `Id` with hash-based change detection. |

## Schedule

Runs as a standalone pipeline at **2:00 AM UTC daily** — earlier than
the main Bronze/Silver pipeline (6:00 AM) to ensure audit data is staged
and ready before the Silver layer processes it.

## Authentication

Uses a Service Principal with `ActivityFeed.Read` permission on the
O365 Management Activity API. Credentials stored in Azure Key Vault,
retrieved via `mssparkutils.secrets.get()` — never hardcoded.

## RecordType 21 — Power BI Events

| Operation Examples | Description |
|-------------------|-------------|
| `ViewReport` | User viewed a Power BI report |
| `ExportReport` | User exported report data |
| `ViewDashboard` | User viewed a dashboard |
| `RefreshDataset` | Dataset refresh triggered |
| `ShareReport` | Report shared with another user |
| `DeleteReport` | Report deleted |
| `DownloadReport` | Report downloaded |
