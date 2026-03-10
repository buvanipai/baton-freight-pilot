# FreightPilot — Build Instructions v2
> Near Real-Time Freight Event Streaming Pipeline with ML Anomaly Detection  
> Target: Baton (A Ryder Technology Lab) Application

---

## Project Goal
Build a production-grade freight data platform that:
1. Streams real-time carrier events through Redis Streams
2. Processes and stores them in PostgreSQL
3. Detects exceptions via SQL rules
4. Detects anomalies via Isolation Forest
5. Predicts revised ETAs via Random Forest
6. Proposes reroutes on-demand via LLM
7. Serves everything via FastAPI
8. Displays via a React dashboard on GitHub Pages

---

## Repo Structure
```
freightpilot/
├── data/
│   └── raw/
│       ├── carrier_allwithhistory.txt     # FMCSA carrier data
│       └── FAF5.7.1_State_2018-2024.csv   # FAF5 freight flow data
├── producer/
│   └── main.py                            # event generator → Redis Stream
├── consumer/
│   └── main.py                            # Redis Stream → PostgreSQL
├── api/
│   ├── main.py                            # FastAPI endpoints
│   ├── agent.py                           # reroute LLM call
│   └── models.py                              # Isolation Forest + Random Forest
├── db/
│   ├── schema.sql                         # PostgreSQL schema
│   └── seed.py                            # FMCSA + FAF5 seed
├── dashboard/
│   ├── index.html                         # React CDN app
│   └── config.js                          # API base URL
├── docker-compose.yml                     # Redis + PostgreSQL
├── .github/
│   └── workflows/
│       └── deploy.yml                     # GitHub Pages CI/CD
├── .env                                   # secrets (gitignored)
├── .env.example                           # template (committed)
├── .gitignore
├── requirements.txt
└── instructions.md
```

---

## Environment Setup

### Always use virtual environment
```bash
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate   # Windows
```

### .gitignore
```
venv/
.env
__pycache__/
*.pyc
*.pyo
data/raw/
*.db
.DS_Store
models/
```

### .env.example
```
ANTHROPIC_API_KEY=your_anthropic_key
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql://freightpilot:freightpilot@localhost:5432/freightpilot
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=freightpilot
```

---

## Requirements (requirements.txt)
```
fastapi
uvicorn
redis
psycopg2-binary
pandas
numpy
scikit-learn
langchain
langchain-anthropic
anthropic
pydantic
python-dotenv
slowapi
httpx
joblib
```

---

## docker-compose.yml
```yaml
version: "3.9"
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: freightpilot
      POSTGRES_USER: freightpilot
      POSTGRES_PASSWORD: freightpilot
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U freightpilot"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  pgdata:
```

---

## PART 1 — Data Layer

### 1.1 db/schema.sql

```sql
CREATE TABLE IF NOT EXISTS carriers (
    dot TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS customers (
    id TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    tier TEXT NOT NULL,
    industry TEXT NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
    id TEXT PRIMARY KEY,
    carrier_dot TEXT REFERENCES carriers(dot),
    customer_id TEXT REFERENCES customers(id),
    origin_state TEXT NOT NULL,
    destination_state TEXT NOT NULL,
    mode TEXT NOT NULL,
    commodity TEXT NOT NULL,
    status TEXT NOT NULL,
    carrier_status TEXT DEFAULT 'active',
    freight_value_usd REAL NOT NULL,
    weight_tons REAL NOT NULL,
    eta_hours REAL NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    last_update TIMESTAMPTZ NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    predicted_eta_hours REAL,
    anomaly_score REAL,
    is_anomaly BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    shipment_id TEXT REFERENCES shipments(id),
    event_type TEXT NOT NULL,
    mode TEXT NOT NULL,
    location TEXT,
    note TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    stream_id TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    events_processed INTEGER DEFAULT 0,
    events_per_second REAL DEFAULT 0,
    consumer_lag INTEGER DEFAULT 0,
    exceptions_detected INTEGER DEFAULT 0,
    anomalies_detected INTEGER DEFAULT 0,
    processing_latency_ms REAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);
CREATE INDEX IF NOT EXISTS idx_shipments_carrier ON shipments(carrier_dot);
CREATE INDEX IF NOT EXISTS idx_shipments_created ON shipments(created_at);
CREATE INDEX IF NOT EXISTS idx_events_shipment ON events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
```

---

### 1.2 db/seed.py

**Purpose:** Load FMCSA carriers + FAF5 freight flows, generate initial shipment population.

**Step 1 — Load FMCSA carriers:**
- Read `data/raw/carrier_allwithhistory.txt` with `csv.reader`, encoding `latin-1`
- Column indices (0-based):
  - 1 = DOT number
  - 4 = Common Authority (`A`=active, `I`=inactive)
  - 25 = Company name
  - 29 = City
  - 30 = State
- Filter: authority == `'A'`, name not empty
- Deduplicate by DOT number
- Sample 2000 carriers randomly (`random.seed(42)`)
- Insert into `carriers` table

**Step 2 — Load FAF5 freight flows:**
- Read `data/raw/FAF5.7.1_State_2018-2024.csv` with pandas
- Keep all modes (do NOT filter by mode)
- Filter: `value_2024 > 0` and `tons_2024 > 0`
- Map `dms_origst` and `dms_destst` using this FIPS → state name dict:
```python
FIPS_TO_STATE = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'Washington DC', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
}
```
- Map `dms_mode` using:
```python
MODE_MAP = {
    '1': 'truck', '2': 'rail', '3': 'water',
    '4': 'air', '5': 'multimodal', '6': 'pipeline', '7': 'other'
}
```
- Map `sctg2` using:
```python
COMMODITY_MAP = {
    '01': 'Live animals/fish', '02': 'Cereal grains', '03': 'Other ag prods.',
    '04': 'Animal feed', '05': 'Meat/seafood', '06': 'Milled grain prods.',
    '07': 'Other foodstuffs', '08': 'Alcoholic beverages', '09': 'Tobacco prods.',
    '10': 'Building stone', '11': 'Natural sands', '12': 'Gravel',
    '13': 'Nonmetallic minerals', '14': 'Metallic ores', '15': 'Coal',
    '16': 'Crude petroleum', '17': 'Gasoline', '18': 'Fuel oils',
    '19': 'Natural gas', '20': 'Basic chemicals', '21': 'Pharmaceuticals',
    '22': 'Fertilizers', '23': 'Chemical prods.', '24': 'Plastics/rubber',
    '25': 'Logs', '26': 'Wood prods.', '27': 'Newsprint/paper',
    '28': 'Paper articles', '29': 'Printed prods.', '30': 'Textiles/leather',
    '31': 'Nonmetal min. prods.', '32': 'Base metals', '33': 'Articles-base metal',
    '34': 'Machinery', '35': 'Electronics', '36': 'Motorized vehicles',
    '37': 'Transport equip.', '38': 'Precision instruments', '39': 'Furniture',
    '40': 'Misc. mfg. prods.', '41': 'Waste/scrap', '43': 'Mixed freight'
}
```
- Keep columns: `origin_state`, `destination_state`, `mode`, `commodity`, `value_2024`, `tons_2024`
- Drop rows where origin == destination
- This becomes your `faf_lanes` dataframe

**Step 3 — Generate customers:**
- Generate 500 customers
- Tiers: 60% small (1-5 shipments), 30% medium (10-25), 10% large (50-100)
- Company name format: `f"{random_city} {random_industry} {random_suffix}"`
- Industries: `['Logistics', 'Manufacturing', 'Distribution', 'Trading', 'Supply Co', 'Imports', 'Exports', 'Industrial']`
- Use real US city names (at least 50)
- Insert into `customers` table

**Step 4 — Generate shipments:**
- For each customer generate shipments per their tier
- For each shipment:
  - Sample one row from `faf_lanes` weighted by `value_2024`
  - Assign random carrier from carriers list
  - `freight_value_usd` = `(value_2024 * 1_000_000 / 365) * random(0.001, 0.01)`
  - `weight_tons` = `(tons_2024 * 1000 / 365) * random(0.001, 0.01)`
  - ETA based on mode:
    - truck: random(4, 72) hours
    - rail: random(24, 168) hours
    - air: random(2, 24) hours
    - water: random(72, 720) hours
    - multimodal: random(24, 240) hours
    - pipeline/other: random(1, 48) hours
  - Status assignment (random with probabilities):
    - 85% normal: status = random of `['in_transit', 'picked_up', 'out_for_delivery', 'delivered']`, `carrier_status='active'`, `last_update = now - random(0,2) hours`, `created_at = now - random(2,12) hours`
    - **CRITICAL:** for normal shipments `eta_hours = hours_since_created + random(4, 24)` — ETA must always be in the future
    - 7% delayed: `status='delayed'`, `carrier_status='active'`, `created_at = now - 20 hours`, `eta_hours=14`, `last_update = now - 1 hour`
    - 5% silent: `status='in_transit'`, `carrier_status='active'`, `last_update = now - random(7,12) hours`, `created_at = now - random(12,24) hours`, eta in future
    - 3% unresponsive: `status='in_transit'`, `carrier_status='unresponsive'`, `last_update = now - random(2,5) hours`
  - Shipment ID format: `SH-{i:05d}`

**Step 5 — Generate events:**
- For each shipment generate 2-4 events
- Event sequences:
  - normal: `['shipment_created', 'picked_up', 'in_transit', 'out_for_delivery']`
  - delayed: `['shipment_created', 'picked_up', 'in_transit', 'delay_reported']`
  - silent: `['shipment_created', 'picked_up', 'in_transit', 'carrier_silent']`
  - unresponsive: `['shipment_created', 'picked_up', 'in_transit']`
- Timestamps sequential from `created_at`

**Step 6 — Print summary:**
```
Loaded 2,000 FMCSA carriers
Loaded {n} FAF5 freight lanes across all modes
Generated 500 customers
Seeded {n} shipments:
  - {n} normal       ({pct}%)
  - {n} delayed      ({pct}%)
  - {n} silent       ({pct}%)
  - {n} unresponsive ({pct}%)
  - {n} delivered    ({pct}%)
Mode breakdown:
  - truck: {n}
  - rail: {n}
  - air: {n}
  - water: {n}
  - multimodal: {n}
  - other: {n}
Total events: {n}
```

Use `psycopg2` for PostgreSQL. Connection from `DATABASE_URL` env var.
Idempotent: `TRUNCATE carriers, customers, shipments, events, pipeline_metrics CASCADE` before inserting.
Run `schema.sql` first if tables don't exist.

---

## PART 2 — Redis Streams

### 2.1 producer/main.py

**Purpose:** Continuously generate carrier events → publish to Redis Stream.

- Connect to Redis via `REDIS_URL`
- Stream name: `freight-events`
- Load shipment IDs from PostgreSQL on startup
- Every 2-5 seconds generate 1-3 events
- Each event dict:
```python
{
    "shipment_id": "SH-00123",
    "event_type": "in_transit",
    "mode": "truck",
    "location": "Memphis, TN",
    "note": "On schedule",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "freight_value_usd": "45000.00",
    "carrier": "XPO LOGISTICS",
}
```
- Publish: `r.xadd("freight-events", event_dict)`
- Randomly inject exceptions (10% of events):
  - Delay: publish `delay_reported`, update `eta_hours` in DB
  - Unresponsive: set `carrier_status='unresponsive'` in DB
- Graceful shutdown on `KeyboardInterrupt`

---

### 2.2 consumer/main.py

**Purpose:** Read Redis Stream → validate → upsert PostgreSQL.

- Connect to Redis + PostgreSQL on startup
- Create consumer group: `r.xgroup_create("freight-events", "freightpilot-consumers", id="0", mkstream=True)` — `try/except` if exists
- Consumer name: `consumer-1`
- Read: `r.xreadgroup("freightpilot-consumers", "consumer-1", {"freight-events": ">"}, count=10, block=5000)`
- Per message:
  1. Validate required fields — skip invalid
  2. Insert into `events` table with `stream_id` = Redis message ID
  3. Update `shipments.last_update`
  4. Update `shipments.status` based on event_type
  5. Acknowledge: `r.xack("freight-events", "freightpilot-consumers", message_id)`
- Every 30s insert row into `pipeline_metrics`:
  - `events_per_second` = events / 30
  - `consumer_lag` = `r.xpending()` count
  - `processing_latency_ms` = average per event
- Graceful shutdown on `KeyboardInterrupt`

---

## PART 3 — ML Layer (api/ml.py)

### Isolation Forest — Anomaly Detection

```python
class AnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(contamination=0.05, random_state=42)
        self.is_fitted = False

    def fit(self, df: pd.DataFrame) -> None:
        # Features: eta_hours, freight_value_usd, weight_tons,
        #           hours_elapsed, mode_encoded, status_encoded
        # Scale with StandardScaler

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        # Returns anomaly scores — negative = more anomalous

    def fit_and_score_all(self, conn) -> None:
        # Load all non-resolved shipments
        # Fit, score, update shipments.anomaly_score + is_anomaly
        # is_anomaly = True if prediction == -1
```

Feature: `hours_elapsed = (now - created_at).total_seconds() / 3600`
Save model: `joblib.dump` to `models/anomaly_detector.joblib`
Load on startup: `joblib.load` if file exists

---

### Random Forest — ETA Prediction

```python
class ETAPredictor:
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.is_fitted = False

    def fit(self, conn) -> None:
        # Train on delivered shipments only (ground truth)
        # Features: mode_encoded, origin_state_encoded, destination_state_encoded,
        #           freight_value_usd, weight_tons, hour_of_day, day_of_week
        # Target: actual_hours (created_at to last_update)
        # Skip if fewer than 50 delivered shipments

    def predict_one(self, shipment: dict) -> float:
        # Returns predicted hours

    def update_predictions(self, conn) -> None:
        # Score all non-delivered shipments
        # Update shipments.predicted_eta_hours
```

Save model: `joblib.dump` to `models/eta_predictor.joblib`

---

## PART 4 — FastAPI (api/main.py)

Use `lifespan` context manager for startup/shutdown:
- Startup: init DB connection, fit ML models
- Shutdown: close connections

**Endpoints:**

`GET /health`
- Check PostgreSQL + Redis
- Return: `{"status": "ok", "db": "connected", "redis": "connected", "timestamp": ...}`

`GET /shipments`
- Params: `limit=100`, `offset=0`, `mode=None`, `status=None`
- Return: `{"shipments": [...], "total": n, "limit": 100, "offset": 0}`
- Include `predicted_eta_hours`, `anomaly_score`, `is_anomaly`

`GET /shipments/{shipment_id}`
- Full shipment + last 5 events

`GET /exceptions`
- SQL detection:
  - Delayed: `(now - created_at) hours - eta_hours > 4` AND status != 'delivered'
  - Silent: `(now - last_update) hours > 6` AND carrier_status != 'unresponsive' AND status != 'delivered'
  - Unresponsive: `carrier_status = 'unresponsive'` AND status != 'delivered'
  - Anomaly: `is_anomaly = TRUE` AND status != 'delivered'
- Sort by `freight_value_usd DESC`
- Rate limit: 30/minute

`POST /reroute/{shipment_id}`
- Fetch shipment context
- Call Claude Haiku (see agent.py)
- Rate limit: 20/minute

`GET /pipeline/metrics`
- Last 20 rows from `pipeline_metrics`

`GET /pipeline/stream-info`
- Redis stream length, consumer lag, last entry ID

`GET /customers`
- All customers: id, company_name, tier, industry, state

`POST /resolve/{shipment_id}`
- Set resolved=TRUE, resolved_at=now

`POST /ml/retrain`
- Background task: refit both models
- Rate limit: 5/minute

---

## PART 5 — Reroute Agent (api/agent.py)

Direct LLM call — no agent loop.

```python
def propose_reroute(shipment_context: dict) -> list[dict]:
    # Call Claude Haiku
    # Return 3 reroute options
```

Prompt instructs model to consider ALL modes (truck, rail, air, water, multimodal) for alternatives.

Each option:
```json
{
  "carrier_alt": "string",
  "mode_alt": "string",
  "estimated_cost_delta_usd": float,
  "new_eta_hours": float,
  "risk_level": "low|medium|high",
  "reasoning": "string"
}
```

Retry once on JSON parse failure. Fallback on second failure.

---

## PART 6 — Dashboard (dashboard/index.html)

Single file React CDN app.

**Components:**
```
App
├── Header                — title + pipeline status
├── PipelineMetrics       — events/sec, consumer lag, latency (auto-refresh 10s)
├── ExceptionsPanel       — fetches /exceptions
│   └── ExceptionCard     — expandable per exception
│       └── RerouteOptions — fetches /reroute/{id} on demand
└── ShipmentsTable        — paginated 100/page, filter by mode/status (auto-refresh 30s)
```

**Row colors:**
- `status === 'delayed'` → `bg-red-100`
- `carrier_status === 'unresponsive'` → `bg-orange-100`
- silent (JS: last_update > 6hrs ago) → `bg-yellow-100`
- `is_anomaly === true` → `bg-purple-100`
- normal → `bg-green-50`

**Exception cards:**
- Sort by `freight_value_usd DESC`
- "🔀 Propose Reroute" → `POST /reroute/{id}` → show 3 options with loading spinner
- "✓ Resolve" → `POST /resolve/{id}`

**Shipments table columns:**
ID, Carrier, Route, Mode, Commodity, Status, Last Update, ETA, Predicted ETA, Value, Anomaly

---

## PART 7 — CI/CD (.github/workflows/deploy.yml)

```yaml
name: Deploy Dashboard to GitHub Pages
on:
  push:
    branches: [main]
    paths:
      - "dashboard/**"
permissions:
  contents: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./dashboard
          publish_branch: gh-pages
          commit_message: "deploy: ${{ github.sha }}"
```

---

## How to Run Locally

```bash
# 1. Activate venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start Redis + PostgreSQL
docker compose up -d

# 4. Seed DB (wait for containers healthy first)
python db/seed.py

# 5. Start consumer (terminal 1)
python consumer/main.py

# 6. Start producer (terminal 2)
python producer/main.py

# 7. Start API (terminal 3)
uvicorn api.main:app --reload --port 8000

# 8. Open dashboard (terminal 4)
python -m http.server 3000 --directory dashboard
```

---

## Build Order for Copilot — One Phase at a Time

Build one phase completely before moving to the next. Show output and wait for confirmation before proceeding.

**Phase 1 — Data Layer**
`docker-compose.yml`, `db/schema.sql`, `db/seed.py`
Verify: `docker compose up -d` → `python db/seed.py` prints correct summary

**Phase 2 — Streaming Pipeline**
`consumer/main.py`, `producer/main.py`
Verify: producer publishes events → consumer reads and writes to PostgreSQL

**Phase 3 — API + ML + Rerouting**
`api/models.py`, `api/agent.py`, `api/main.py`
Verify: `uvicorn api.main:app --reload` → `GET /health` returns 200 → `GET /exceptions` returns correct exceptions

**Phase 4 — Dashboard**
`dashboard/config.js`, `dashboard/index.html`
Verify: `python -m http.server 3000 --directory dashboard` → table loads → exceptions panel works → reroute button works

**Phase 5 — Infra/Config**
`.github/workflows/deploy.yml`, `requirements.txt`, `.env.example`, `.gitignore`
Verify: push to GitHub → Actions deploys to GitHub Pages

---

## Production Considerations

| Area | This project | Production |
|------|-------------|------------|
| Streaming | Redis Streams | Kafka + Kafka Streams |
| Database | PostgreSQL (Docker) | PostgreSQL on RDS/Cloud SQL |
| ML training | On-demand API | Scheduled Airflow DAG |
| ML serving | In-process | Separate model server |
| Consumer scaling | Single process | Consumer group + multiple workers |
| Monitoring | pipeline_metrics table | Prometheus + Grafana |
| Deployment | Railway + GitHub Pages | GCP Cloud Run + CDN |
| Secrets | .env | GCP Secret Manager |

---

## Data Sources

| Data | Source | Notes |
|------|--------|-------|
| Carrier names, DOT#, status | FMCSA Carrier All With History | 325K+ real US carriers |
| Freight lanes, values, commodities | FAF5.7.1 State DB 2018-2024 | 1.1M rows, all transport modes |
| Shipment IDs, timestamps, events | Synthetic | Only synthetic component |

> In production: events come from carrier EDI feeds (X12 214), TMS webhooks, or ELD telemetry.

---

## Notes for Copilot

- Always activate and use `venv/`
- Always load `.env` with `python-dotenv`
- Use `psycopg2` for PostgreSQL — not SQLAlchemy
- Use `redis-py` for Redis
- All timestamps timezone-aware: `TIMESTAMPTZ` in PostgreSQL, `datetime.now(timezone.utc)` in Python
- `seed.py` idempotent: `TRUNCATE ... CASCADE` before inserting
- Producer and consumer are standalone Python processes — never imported by API
- ML models saved to `models/` directory — create if not exists
- Dashboard fetches customers from `GET /customers` — never hardcode
- Reroute proposals consider ALL transport modes
- `dms_origst` and `dms_destst` in FAF5 are zero-padded strings e.g. `'01'` not `1` — handle carefully when mapping