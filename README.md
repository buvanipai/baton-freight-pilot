# FreightPilot — Baton Freight Intelligence Platform

> Near real-time freight event streaming pipeline with ML anomaly detection, ETA prediction, and LLM-powered rerouting. Built as a take-home project targeting [Baton (A Ryder Technology Lab)](https://baton.com).

**Live Dashboard → [buvanipai.github.io/baton-freight-pilot](https://buvanipai.github.io/baton-freight-pilot/)**  
**API → [freightpilot-api-t75n.onrender.com](https://freightpilot-api-t75n.onrender.com/docs)**

> The API runs on Render's free tier and spins down after 15 minutes of inactivity. First load may take 30–60 seconds to cold-start.

---

## What It Does

| Layer | Tech | Detail |
|---|---|---|
| **Data** | FMCSA + FAF5 | 7,000+ real shipments seeded from federal freight datasets |
| **Streaming** | Redis Streams | Producer publishes carrier events every 2–5s; consumer processes at 4–10ms latency |
| **Storage** | PostgreSQL 15 | Shipments, carriers, customers, events tables |
| **Anomaly Detection** | IsolationForest | 200 estimators, 15% contamination, 6 features — flags ~877 anomalies across 5,853 active shipments |
| **ETA Prediction** | RandomForestRegressor | 100 estimators, trained on 1,597 delivered shipments |
| **Exception Detection** | SQL rules | Delayed, silent (no update > 2h), unresponsive carrier, anomaly |
| **Rerouting Agent** | Claude (LangChain) | Proposes 3 reroute options with carrier, mode, risk level, cost delta, and reasoning |
| **API** | FastAPI | Rate-limited REST API with connection pooling |
| **Frontend** | React 18 + Tailwind | Single-page dashboard on GitHub Pages |

---

## Architecture

```
FMCSA / FAF5 data
       │
       ▼
   db/seed.py ──────────────────────► PostgreSQL
                                           │
Producer (daemon thread)                   │
  generates carrier events                 │
       │                                   │
       ▼                                   │
 Redis Stream ──► Consumer (daemon) ──────►│
                                           │
                                     FastAPI (uvicorn)
                                      ├── /health
                                      ├── /shipments
                                      ├── /exceptions
                                      ├── /pipeline/metrics
                                      ├── /reroute/{id}   ◄── Claude Agent
                                      └── /resolve/{id}
                                           │
                                    GitHub Pages (React)
```

Producer, consumer, and ML model fitting all run as daemon threads inside the single FastAPI process (required for Render's free tier single-service constraint).

---

## Project Structure

```
├── api/
│   ├── main.py          # FastAPI app, lifespan threads, endpoints
│   ├── agent.py         # LangChain + Claude reroute proposals
│   └── models.py        # IsolationForest + RandomForest training & inference
├── producer/
│   └── main.py          # Redis Streams event publisher
├── consumer/
│   └── main.py          # Redis Streams consumer → PostgreSQL writer
├── db/
│   ├── schema.sql        # PostgreSQL schema (idempotent)
│   ├── seed.py           # FMCSA + FAF5 seeder
│   ├── seed_dump.sql     # 3.4MB SQL dump (replaces 765MB raw files)
│   ├── seed_restore.py   # Restores seed_dump.sql via psql subprocess
│   └── schema_apply.py  # Applies schema idempotently on startup
├── docs/
│   ├── index.html        # React dashboard (GitHub Pages)
│   └── config.js         # API base URL (localhost vs Render)
├── data/raw/             # gitignored — FMCSA + FAF5 source files
├── models/               # gitignored — fitted .joblib model files
├── docker-compose.yml    # Local PostgreSQL + Redis
├── render.yaml           # Render Blueprint (all services)
└── requirements.txt
```

---

## Running Locally

**Prerequisites:** Docker, Python 3.11+

```bash
# 1. Start PostgreSQL + Redis
docker-compose up -d

# 2. Install dependencies
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Set environment variables
cp .env.example .env
# Fill in ANTHROPIC_API_KEY

# 4. Apply schema and seed data
python db/schema_apply.py
python db/seed.py          # requires data/raw/ files (see note below)
# OR restore the committed dump (no raw files needed):
python db/seed_restore.py

# 5. Start the API (producer + consumer start automatically)
uvicorn api.main:app --reload --port 8000
```

> **Raw data files** (`data/raw/`) are gitignored due to size (765MB combined). `db/seed_dump.sql` (3.4MB) is included as a pre-seeded alternative.

Open `docs/index.html` directly in a browser or serve it with any static file server.

---

## Key Design Decisions

- **Single Render service** — Render's free tier doesn't support worker services. Producer, consumer, and model fitting run as daemon threads inside the web process, started via FastAPI's lifespan context manager.
- **Seed dump over raw data** — The FMCSA and FAF5 source files are 470MB + 295MB. A `pg_dump` of the seeded database is 3.4MB and committed to the repo, making cold deploys fast.
- **Deferred model fitting** — IsolationForest and RandomForest are fitted in a background thread at startup so uvicorn binds its port immediately (avoids Render's port scan timeout).
- **GitHub Pages for frontend** — The React app is a single HTML file in `docs/` served natively by GitHub Pages. No build step, no Node.js required.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | DB + Redis status |
| GET | `/shipments` | Paginated shipment list with filters + sort |
| GET | `/shipments/{id}` | Single shipment + last 5 events |
| GET | `/exceptions` | Active exceptions (delayed, silent, unresponsive, anomaly) |
| GET | `/pipeline/metrics` | Throughput, latency, exception counts |
| GET | `/pipeline/stream-info` | Redis stream length + consumer lag |
| POST | `/reroute/{id}` | Claude agent proposes 3 reroute options |
| POST | `/resolve/{id}` | Mark exception as resolved |

Interactive docs: `https://freightpilot-api-t75n.onrender.com/docs`

---

## Data Sources

- **FMCSA Motor Carrier Census** — Real US carrier names, DOT numbers, operating authority, safety ratings
- **FAF5 Freight Analysis Framework** — State-to-state freight flows (2018–2024), commodity codes, tonnage, value by mode
