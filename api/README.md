API service (FastAPI)

- Install: pip install -r requirements.txt
- Run: uvicorn app.main:app --host 0.0.0.0 --port 8000
- Health: GET /health

This service loads environment variables via the shared Settings from ../shared/python/monorepo_common.
It will automatically read a .env file located at the repository root if present.
