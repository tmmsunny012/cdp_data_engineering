# Customer Data Platform (CDP) — EdTech Platform

> A production-grade Customer Data Platform that consolidates student data from 5 sources into unified profiles, enabling real-time personalization, AI-powered engagement, and GDPR-compliant data management.

## Architecture Overview

```
Sources (5)          Ingestion           Processing          Storage              Serving
─────────────       ──────────          ──────────          ───────              ───────
Website (JSON)  ─→                                          MongoDB
Mobile App (JSON)─→  Kafka     ─→  Stream Processor  ─→  (Real-time     ─→  Profile API
Salesforce (CDC) ─→  + Schema     Identity Resolution     Profiles)          Segmentation
Email (Webhooks) ─→  Registry     Profile Builder                             Reverse ETL
WhatsApp (Text)  ─→                                        BigQuery
                                                          (Analytics,     ─→  Vertex AI
                                                           Medallion)        Pinecone
```

## Key Features

| Feature | Implementation |
|---------|---------------|
| **5 Data Sources** | Website, Mobile App, Salesforce CRM, Email Marketing, WhatsApp (Twilio) |
| **Multi-Format Ingestion** | JSON, CSV, unstructured text — all normalized via `format_normalizer.py` |
| **Near Real-Time** | Kafka streaming → profile update in < 5 minutes |
| **Identity Resolution** | Deterministic (email/phone) + probabilistic (name/fuzzy) matching |
| **Unified Profiles** | MongoDB for real-time serving, BigQuery for analytics |
| **Medallion Architecture** | Bronze (raw) → Silver (cleaned) → Gold (enriched) in BigQuery |
| **AI/ML Ready** | Vertex AI Feature Store + Pinecone vector search |
| **GDPR Compliant** | Consent management, PII encryption, right-to-forget cascade deletion |
| **Data Quality** | 4 Great Expectations gates at every layer transition |
| **Reverse ETL** | BigQuery → Salesforce, Twilio, Email platform (with consent checks) |

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Streaming | Apache Kafka + Schema Registry |
| Profile Store | MongoDB Atlas |
| Analytics Warehouse | Google BigQuery |
| API Layer | FastAPI (async Python) |
| Orchestration | Apache Airflow |
| AI/ML | Vertex AI Feature Store + Pinecone |
| Data Quality | Great Expectations |
| Privacy | Custom GDPR engine + GCP KMS |
| Monitoring | Prometheus + Grafana + PagerDuty |
| CI/CD | GitHub Actions + Docker |
| Language | Python 3.11+ (typed, async, Pydantic v2) |

## Project Structure

```
cdp_data_engineering/
├── docs/
│   ├── presentation.md              # Architecture presentation (21 slides)
│   └── interview_qa.md              # Interview Q&A (120+ questions, 17 categories)
├── src/
│   ├── common/
│   │   ├── logging_config.py        # Structured JSON logging, PII redaction
│   │   └── metrics.py               # Prometheus metrics + FastAPI middleware
│   ├── cli/
│   │   └── pipeline_generator.py    # CLI for generating new ETL/ELT connectors
│   ├── ingestion/
│   │   ├── kafka_producer.py        # Generic async Kafka producer
│   │   ├── salesforce_connector.py  # Salesforce CDC + CSV bulk ingestion
│   │   ├── twilio_webhook.py        # WhatsApp unstructured text handler
│   │   ├── email_webhook.py         # Email marketing webhook (opens/clicks/bounces)
│   │   ├── clickstream_consumer.py  # Website JSON clickstream consumer
│   │   ├── mobile_app_consumer.py   # Mobile app events + device IDs
│   │   └── format_normalizer.py     # Central: JSON/CSV/Text → unified schema
│   ├── processing/
│   │   ├── stream_processor.py      # Async Kafka stream processor
│   │   ├── identity_resolution.py   # Deterministic + probabilistic matching
│   │   └── profile_builder.py       # Unified profile assembly
│   ├── storage/
│   │   ├── mongodb_profile_store.py # MongoDB async CRUD for profiles
│   │   ├── bigquery_loader.py       # BigQuery streaming + batch loads
│   │   └── models/
│   │       └── customer_profile.py  # Pydantic unified profile model
│   ├── serving/
│   │   ├── profile_api.py           # FastAPI async profile retrieval
│   │   ├── segmentation_engine.py   # Real-time segment evaluation
│   │   └── reverse_etl.py           # BigQuery → Salesforce/Twilio/Email
│   ├── ml/
│   │   ├── vertex_feature_store.py  # Vertex AI Feature Store operations
│   │   ├── pinecone_manager.py      # Pinecone vector upsert + search
│   │   └── embedding_generator.py   # Vertex AI text-embedding generation
│   ├── privacy/
│   │   ├── consent_manager.py       # GDPR consent per-channel management
│   │   └── gdpr_deletion.py         # Right-to-forget cascade across all stores
│   ├── quality/
│   │   └── data_quality_checks.py   # Great Expectations 4-gate validation
│   └── orchestration/
│       └── cdp_dag.py               # Airflow DAG for batch pipelines
├── tests/                           # Unit + integration + quality tests
├── .github/
│   ├── workflows/ci.yml             # GitHub Actions CI pipeline
│   └── CODEOWNERS                   # Code review ownership
├── .pre-commit-config.yaml          # Pre-commit hooks
├── docker-compose.yml               # Local dev environment
├── Dockerfile                       # Service container
├── pyproject.toml                   # Python project config
├── requirements.txt                 # Dependencies
└── README.md                        # This file
```

## Quick Start

### Local Development

```bash
# 1. Clone and setup
git clone <repository-url>
cd cdp_data_engineering
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start infrastructure
docker-compose up -d kafka mongodb prometheus grafana

# 3. Run the Profile API
uvicorn src.serving.profile_api:app --reload --port 8000

# 4. Run the stream processor
python -m src.processing.stream_processor

# 5. Run tests
pytest tests/unit/ -v
pytest tests/integration/ -v  # requires Docker services running
```

### Generate a New Connector

```bash
# Streaming connector (JSON source)
python -m src.cli.pipeline_generator generate-connector --name new_lms --type streaming --format json

# Batch connector (CSV source)
python -m src.cli.pipeline_generator generate-connector --name alumni_export --type batch --format csv
```

## Data Flow

```
1. Sources emit events → Kafka topics (cdp.raw.{source})
2. Format Normalizer → unified CustomerEvent schema
3. Stream Processor → Identity Resolution → Profile Builder
4. Bronze (raw) → Silver (cleaned) → Gold (enriched) in BigQuery
5. MongoDB updated with real-time unified profiles
6. Segmentation Engine evaluates rules → triggers marketing actions
7. Reverse ETL pushes insights back to Salesforce/Twilio/Email
8. Vertex AI + Pinecone serve ML predictions + recommendations
```

## Key Design Decisions

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| Kafka over Pub/Sub | Kafka | More control over partitioning + consumer groups; higher operational overhead |
| MongoDB + BigQuery (dual store) | Both | Redundancy for speed (MongoDB) vs. analytics (BigQuery); eventual consistency managed by event-driven updates |
| Great Expectations over dbt tests | GE primary | Works across all stores (Kafka, MongoDB, BigQuery, CSV); dbt only for SQL |
| Python over Java/Scala | Python | Team alignment (data scientists use Python); sufficient performance for this scale |
| Pinecone over pgvector | Pinecone | Fully managed, no ops overhead; vendor lock-in accepted for simplicity |
| Build vs. buy CDP | Build | $3K/month vs. $12K commercial; full control over identity resolution + GDPR |

## SLAs

| Metric | Target |
|--------|--------|
| Profile API latency | p99 < 200ms |
| Streaming data freshness | < 5 minutes |
| Identity match rate | > 85% |
| GDPR deletion | < 72 hours |
| System uptime | 99.9% |

## Documentation

- [Architecture Presentation](docs/presentation.md) — 21-slide CDP architecture with Mermaid diagrams

## License

MIT License
