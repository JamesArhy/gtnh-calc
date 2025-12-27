# GTNH Production Planner

Single-page app + small backend for planning GTNH recipe chains with machine counts.

## Layout
- `backend/`: FastAPI API server with DuckDB over Parquet data
- `frontend/`: Vite + React SPA with Cytoscape graph view
- `in/`: source data (Parquet + `recipes.json`)

## Quickstart (local)

Backend:
```
cd backend
python -m venv .venv
. .venv/Scripts/Activate
python -m pip install -r requirements.txt
python -m app.main
```

Frontend:
```
cd frontend
npm install
npm run dev
```

Or use the helper script (Windows PowerShell):
```
.\dev.ps1
```

First-time install of dependencies:
```
.\dev.ps1 -Install
```

## Configuration

Environment variables (backend):
- `DATA_SOURCE=local` (default)
- `LOCAL_DATA_DIR=in/parquet`
- `RECIPES_JSON=in/recipes.json`
- `MACHINE_INDEX_JSON=in/machine_index.json`
- `DEFAULT_VERSION=local`
- `GRAPH_DB=ladybugdb` (default; options: `ladybugdb`, `real-ladybug`, `duckdb`/`off`)

Notes:
- The backend reads Parquet directly with DuckDB. A name index is built from `recipes.json`.
- S3 support is stubbed; switch `DATA_SOURCE=s3` once implemented.
- `GRAPH_DB=ladybugdb` runs Cypher queries on a real-ladybug graph instance (install required). Use `GRAPH_DB=off` to stick with DuckDB only.
