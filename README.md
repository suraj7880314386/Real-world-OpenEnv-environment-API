---
title: Data Pipeline Debugging Environment
emoji: 🔧
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: false
---

# Data Pipeline Debugging Environment

An OpenEnv environment where AI agents diagnose and fix real-world data pipeline issues. The environment simulates broken ETL pipelines with type mismatches, schema drift, orphan records, and data quality problems.

## Tasks

| Task | Difficulty | Bugs | Max Steps |
|------|-----------|------|-----------|
| `type_mismatch_fix` | Easy | 2 | 10 |
| `schema_drift_repair` | Medium | 3 | 15 |
| `etl_pipeline_overhaul` | Hard | 5 | 20 |

## API Endpoints

- `POST /reset` — Start a new episode
- `POST /step` — Execute an action
- `GET /state` — Get current state
- `GET /health` — Health check

## Action Space

Commands: `inspect_schema`, `inspect_data`, `run_query`, `diagnose`, `fix`, `validate`

## Scoring

Score = bugs_fixed / total_bugs (always 0.0 to 1.0)
