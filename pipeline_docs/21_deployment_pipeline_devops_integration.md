# Fabric Deployment Pipeline & Azure DevOps Integration
# Portfolio Reference: Fabric Workspace Observability Project
# Topic: CI/CD, Environment Promotion, Source Control
#
# Description:
#   Documents the deployment pipeline configuration that separates
#   development from production, enforces promotion gates between
#   environments, and connects the production Fabric workspace to
#   Azure DevOps for full source control of all workspace items.
#
# Tools Used:
#   - Microsoft Fabric Deployment Pipelines (built-in)
#   - Azure DevOps Repos (Git)
#   - Fabric Git Integration (workspace → DevOps sync)

---

## Environment Strategy

```
┌─────────────────────────────────────────────────────────────┐
│  DEVELOPER WORKSTATION                                       │
│  Local editing via Fabric web UI or VS Code + Fabric ext.   │
└──────────────────────┬──────────────────────────────────────┘
                       │ develop & test
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  DEV WORKSPACE  (Fabric)                                     │
│  fabric-workspace-dev                                        │
│                                                              │
│  Purpose: Active development, experimentation, testing       │
│  Data:    Subset / anonymised sample data                    │
│  Access:  Data Engineering team only                         │
│  Refresh: On-demand / manual triggers                        │
│  Git:     NOT directly connected to DevOps                   │
│           (changes promoted via Deployment Pipeline)         │
└──────────────────────┬──────────────────────────────────────┘
                       │ Fabric Deployment Pipeline
                       │ (manual promotion with review)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  PROD WORKSPACE  (Fabric)                                    │
│  fabric-workspace-prod                                       │
│                                                              │
│  Purpose: Production-grade, business-facing                  │
│  Data:    Full production data                               │
│  Access:  Controlled — Data Eng team + read-only consumers   │
│  Refresh: Scheduled pipelines (6am, 7am, 2am)               │
│  Git:     Connected to Azure DevOps Repo                     │
│           Every item in Prod is version-controlled           │
└──────────────────────┬──────────────────────────────────────┘
                       │ Fabric Git Integration (auto-sync)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  AZURE DEVOPS REPO                                           │
│  Organisation/Project/fabric-workspace-prod                  │
│                                                              │
│  Branch: main  (reflects current Prod state)                 │
│  Every promoted item committed with timestamp + author       │
│  Full history of all workspace item changes                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Fabric Deployment Pipeline Configuration

### Pipeline Stages

| Stage | Workspace | Purpose |
|-------|-----------|---------|
| Development | `fabric-workspace-dev` | Build and test new features |
| Production | `fabric-workspace-prod` | Live, scheduled, business-facing |

> **Note:** A UAT/Staging stage was considered but not implemented
> given the team size and the scope of the project. For a larger team
> or more complex release cadence, a 3-stage pipeline
> (Dev → UAT → Prod) would be the recommended pattern.

### Items Managed by Deployment Pipeline

| Item Type | Dev Item | Prod Item | Notes |
|-----------|----------|-----------|-------|
| Notebooks | All `/semantic_model/`, `/audit_logs/`, `/metadata/`, `/validation/`, `/pipeline_docs/` notebooks | Promoted copies | Notebook parameters (Lakehouse names, URIs) configured per-workspace via environment variables |
| Dataflow Gen2 | Gold layer dataflows | Promoted copies | M query connections updated post-promotion to point to Prod Lakehouse |
| Data Pipelines | Bronze Master, Silver Master, Gold+SM Master, Audit Pipeline | Promoted copies | Linked Service references updated for Prod connections |
| Lakehouses | BronzeLakehouse, SilverLakehouse, GoldLakehouse | Separate Prod instances | Data is NOT promoted — only structure/schema |
| Warehouses | SilverWarehouse (control_table) | Separate Prod instance | control_table seeded separately in Prod |
| Semantic Model | .pbix semantic model | Promoted copy | Connection updated to Prod Gold Lakehouse post-promotion |
| Reports | All Power BI reports | Promoted copies | Automatically rebind to Prod semantic model |
| Environments | Spark environment (sempy, kusto connector) | Promoted copy | Library versions locked — same in Dev and Prod |

### What Does NOT Get Promoted (by design)

| Item | Reason |
|------|--------|
| Actual data (Delta tables) | Data lives in each workspace independently — only structure is promoted |
| control_table data rows | Prod control_table is seeded and maintained separately — Dev rows use Dev connection names |
| Secrets / Key Vault references | Each workspace references its own Key Vault scope |
| Scheduled trigger configs | Pipeline schedules are set independently in Prod after promotion |
| Workspace-level permissions | Access control managed separately per environment |

---

## Promotion Process (Step by Step)

### Pre-Promotion Checklist (Dev)
```
□ All notebooks run successfully end-to-end in Dev workspace
□ Row count validation passes (09_rowcount_validation_pre_post.py)
□ No CRITICAL health flags in column profiling results
□ Gold DDL notebook verified — all precision columns correct
□ Semantic model refreshes cleanly against Dev Gold tables
□ Pipeline audit log shows clean run history (no recent FAILURE entries)
□ Peer review completed (if team > 1)
```

### Promotion Steps
```
1. Open Fabric Deployment Pipeline
   Workspace Settings → Deployment Pipelines → [Pipeline Name]

2. Select items to promote
   - Option A: Promote ALL items (full deployment)
   - Option B: Promote selected items only (targeted deployment)
   For hotfixes: Option B — promote only the affected notebook/pipeline

3. Review deployment diff
   Fabric shows a side-by-side diff of what changed between Dev and Prod
   Review all changes before confirming — this is the gate

4. Deploy to Production
   Click "Deploy to Production"
   Fabric copies item definitions from Dev → Prod workspace

5. Post-promotion configuration (manual steps)
   a. Update Dataflow Gen2 connection strings → Prod Lakehouse
   b. Verify Pipeline Linked Services point to Prod connections
   c. Verify Semantic Model connection → Prod Gold Lakehouse
   d. Re-enable scheduled pipeline triggers (disabled during promotion)
   e. Run Gold DDL notebook once manually to confirm schema in Prod
   f. Trigger one manual pipeline run end-to-end to validate

6. Verify in Prod
   □ Bronze pipeline runs and loads to Prod Bronze Lakehouse
   □ Silver pipeline runs cleanly
   □ Gold DDL + Dataflow Gen2 + Gold DML complete
   □ Semantic model refreshes
   □ Row count validation passes in Prod
   □ Reports load correctly

7. Git sync confirms (automatic)
   Prod workspace auto-syncs to Azure DevOps main branch
   Commit appears in DevOps repo with timestamp and deployer identity
```

---

## Azure DevOps Git Integration

### Repository Structure (as committed from Prod workspace)

```
fabric-workspace-prod/                    ← Azure DevOps Repo root
│
├── .platform/                            ← Fabric workspace metadata
│   └── config.json                       ← Workspace settings
│
├── BronzeLakehouse.Lakehouse/            ← Lakehouse item definition
├── SilverLakehouse.Lakehouse/
├── GoldLakehouse.Lakehouse/
├── SilverWarehouse.Warehouse/
│
├── Notebooks/
│   ├── 01_gold_semantic_model_logs_parsed.py
│   ├── 02_semantic_model_system_tables_live.py
│   ├── 03_gold_semantic_model_usage_analytics.py
│   ├── 07_bronze_audit_logs_graph_api.py
│   ├── 08_silver_audit_logs_ddl_dml.py
│   ├── 09_rowcount_validation_pre_post.py
│   ├── 10_control_table_ddl_silver_master.py
│   ├── 14_metadata_master_full_lineage.py
│   ├── 15_column_profiling_metadata_driven.py
│   ├── 16_pipeline_audit_log_ddl_audit_sp.py
│   ├── 17_bronze_master_pipeline_reference.py
│   ├── 18_gold_ddl_precision_tables.py
│   └── 19_gold_dml_merge_staging_to_gold.py
│
├── DataPipelines/
│   ├── Master_Pipeline.DataPipeline/
│   │   └── pipeline-content.json
│   ├── Bronze_Master_Pipeline.DataPipeline/
│   │   └── pipeline-content.json
│   ├── Bronze_Full_Refresh_Pipeline.DataPipeline/
│   │   └── pipeline-content.json
│   ├── Bronze_Incremental_Pipeline.DataPipeline/
│   │   └── pipeline-content.json
│   ├── Gold_SemanticModel_Master_Pipeline.DataPipeline/
│   │   └── pipeline-content.json
│   └── AuditLog_Bronze_Pipeline.DataPipeline/
│       └── pipeline-content.json
│
├── Dataflows/
│   ├── DF_RefreshSummary.Dataflow/
│   │   └── mashup.pq                     ← M query source — fully version-controlled
│   ├── DF_DAXPerformance.Dataflow/
│   │   └── mashup.pq
│   ├── DF_AuditActivity.Dataflow/
│   │   └── mashup.pq
│   └── DF_ModelHealthScorecard.Dataflow/
│       └── mashup.pq
│
├── SemanticModels/
│   └── FabricObservability.SemanticModel/
│       ├── definition.pbidataset
│       ├── model.bim                     ← Full semantic model definition
│       └── Report/
│           └── *.pbireport               ← Report definitions
│
└── Environments/
    └── FabricSparkEnv.Environment/
        └── environment.yml               ← Spark environment + library versions
```

### Key Git Integration Benefits

**M Query versioning (Dataflow Gen2):**
Each Dataflow Gen2 item commits its M query as a `.pq` file to the repo.
This means every Gold layer transformation change is a reviewable Git commit.
You can see exactly what changed in the M query, when, and who promoted it.

```
Example commit message (auto-generated by Fabric Git sync):
  "Workspace sync - 2024-11-14T07:23:41Z [deployer@org.com]
   Modified: Dataflows/DF_RefreshSummary.Dataflow/mashup.pq"
```

**Pipeline JSON versioning:**
Data Pipeline definitions commit as `pipeline-content.json`, capturing
all activity configurations, expressions, linked service references,
and ForEach/Switch/If Condition logic in a diffable format.

**Semantic model versioning:**
The `model.bim` file is the full semantic model definition — all tables,
columns, measures, relationships, and DAX expressions in JSON format.
Any measure change or relationship change is a tracked Git commit.

**Rollback capability:**
Because every Prod state is in Git, rolling back a bad promotion is
a defined process: revert the commit in DevOps → sync back to Fabric
workspace → re-promote the previous version.

---

## Branch Strategy

```
main
  └── Reflects current production state (auto-synced from Prod workspace)
      Read-only from DevOps perspective — changes come via Deployment Pipeline

feature/*  (optional, for teams with multiple developers)
  └── Feature branches for larger changes — merged to dev, tested,
      then promoted through the Deployment Pipeline
```

For this project (small team), all development happened directly in the
Dev workspace and was promoted via the Deployment Pipeline. Feature
branches in DevOps are recommended if the team grows or if multiple
features are developed in parallel.

---

## Why This Architecture Matters

**Without this setup (common mistake):**
- Developers make changes directly in the production workspace
- No record of what changed or when
- A broken notebook or pipeline has no clean rollback path
- Dataflow M query changes are undocumented
- If the workspace is accidentally deleted, everything is lost

**With this setup:**
- Dev workspace absorbs all experimental risk — Prod is always stable
- Every item in Prod has a full commit history in Azure DevOps
- Rolling back any item to a previous state is a defined process
- M query and pipeline logic changes are peer-reviewable
- Workspace items are recoverable from DevOps if something goes wrong
- Promotion is a deliberate, documented act — not an accidental save

---

## Setup Reference (How It Was Configured)

### Step 1: Create Fabric Deployment Pipeline
```
Fabric Portal → Workspaces → Deployment Pipelines → New Pipeline
Name: FabricObservability-DeployPipeline
Stages: Development, Production
Assign workspaces:
  Development stage → fabric-workspace-dev
  Production stage  → fabric-workspace-prod
```

### Step 2: Connect Prod Workspace to Azure DevOps
```
fabric-workspace-prod → Workspace Settings → Git Integration

Provider:          Azure DevOps
Organisation:      [Your DevOps Org]
Project:           [Your DevOps Project]
Repository:        fabric-workspace-prod
Branch:            main
Root folder:       /  (repo root)

Click: Connect and Sync
```

### Step 3: Initial Sync
```
On first connect, Fabric prompts:
  "Sync workspace to Git?" → Yes

This commits all current Prod workspace items to the main branch.
Subsequent promotions auto-commit on sync.
```

### Step 4: DevOps Repo Permissions
```
Ensure the Fabric service principal / managed identity has:
  Contributor access to the Azure DevOps repository

In DevOps:
  Project Settings → Repositories → [Repo] → Security
  Add: [Fabric Workspace MSI] → Contributor
```

### Step 5: Branch Protection (Recommended)
```
In Azure DevOps → Repos → Branches → main → Branch Policies:
  ✔ Require a minimum number of reviewers: 1
  ✔ Check for linked work items (optional)
  ✔ Require up-to-date branches before merging

Note: With Fabric Git sync, the main branch is written by the
Fabric service — branch protection applies to manual PR merges,
not the Fabric sync commits. Configure accordingly.
```
