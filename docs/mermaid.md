# CDP Architecture — Mermaid Diagrams

> 13 individual diagrams, one per slide. Copy-paste into any Mermaid renderer.

---

## 1. Full Overview (Slide 3)

```mermaid
graph LR
    subgraph Sources ["Sources"]
        WEB["Website"] ~~~ MOB["Mobile App"] ~~~ CRM["Salesforce"] ~~~ EMAIL["Email"] ~~~ WA["WhatsApp"]
    end

    subgraph Ingestion ["Ingestion"]
        KAFKA["Kafka"] --> SR["Schema<br/>Registry"] --> FN["Format<br/>Normalizer"]
    end

    subgraph Processing ["Processing"]
        SP["Stream<br/>Processor"] --> IR["Identity<br/>Resolution"] --> PB["Profile<br/>Builder"]
    end

    subgraph Storage ["Storage"]
        MEDAL["Bronze → Silver → Gold"]
        MONGO["MongoDB"] ~~~ BQ["BigQuery"]
    end

    subgraph Serving ["Serving"]
        API["Profile API"] --> SEG["Segmentation"] --> RETL["Reverse ETL"]
    end

    subgraph AI ["AI/ML"]
        VFS["Vertex AI"] ~~~ PC["Pinecone"]
    end

    Sources --> Ingestion --> Processing --> Storage --> Serving
    BQ --> AI
    RETL -.->|sync| Sources
```

---

## 2. Data Sources (Slide 4)

```mermaid
graph LR
    subgraph JSON_Sources ["JSON Sources"]
        WEB["Website<br/>Clickstream<br/><i>session_id, page, UTM</i>"]
        MOB["Mobile App<br/>Events<br/><i>device_id, push_token</i>"]
        EMAIL["Email<br/>Webhooks<br/><i>campaign_id, open/click</i>"]
    end

    subgraph Mixed ["JSON + CSV"]
        CRM["Salesforce CRM<br/>CDC (JSON) + Bulk (CSV)<br/><i>sf_id, email, phone, status</i>"]
    end

    subgraph Unstructured ["Unstructured Text"]
        WA["WhatsApp<br/>Twilio Webhooks<br/><i>phone, raw message text</i>"]
    end

    subgraph Topics ["Kafka Topics"]
        T1["cdp.raw.clickstream"]
        T2["cdp.raw.mobile_app"]
        T3["cdp.raw.email"]
        T4["cdp.raw.crm"]
        T5["cdp.raw.whatsapp"]
    end

    WEB -->|JSON| T1
    MOB -->|JSON| T2
    EMAIL -->|JSON| T3
    CRM -->|JSON/CSV| T4
    WA -->|Text| T5
```

---

## 3. Ingestion Layer (Slide 5)

```mermaid
graph LR
    subgraph Connectors ["Source Connectors"]
        CC["clickstream_consumer.py"]
        MC["mobile_app_consumer.py"]
        SC["salesforce_connector.py"]
        EW["email_webhook.py"]
        TW["twilio_webhook.py"]
    end

    subgraph Kafka ["Apache Kafka"]
        RAW["Raw Topics<br/>cdp.raw.*<br/><i>Partitioned by source ID</i>"]
        PROC["Processed Topics<br/>cdp.processed.*<br/><i>Partitioned by student_id</i>"]
        DLQ["Dead Letter Queue<br/>cdp.dlq<br/><i>Failed events</i>"]
    end

    SR["Schema Registry<br/><i>Backward-compatible<br/>JSON / Avro schemas</i>"]
    FN["Format Normalizer<br/><i>format_normalizer.py</i><br/>JSON / CSV / Text → CustomerEvent"]

    CC --> RAW
    MC --> RAW
    SC --> RAW
    EW --> RAW
    TW --> RAW
    RAW --> SR
    SR -->|valid| FN
    SR -->|invalid| DLQ
    FN --> PROC
```

---

## 4. Processing Layer (Slide 6)

```mermaid
graph TB
    subgraph Stream ["STREAM — Real-Time < 5 min"]
        KF["Kafka<br/>cdp.processed.*"]
        SP["stream_processor.py<br/><i>Async Python</i>"]
        IR1["identity_resolution.py<br/><i>Deterministic match</i>"]
        PB1["profile_builder.py<br/><i>MongoDB upsert</i>"]
        SEG1["segmentation_engine.py<br/><i>Rule evaluation</i>"]
        KF --> SP --> IR1 --> PB1 --> SEG1
    end

    subgraph Batch ["BATCH — Airflow Scheduled"]
        AIR["cdp_dag.py<br/><i>Airflow orchestration</i>"]
        CSV["CSV Bulk Imports<br/><i>salesforce_connector.py</i>"]
        DBT["dbt Transforms<br/><i>Bronze → Silver → Gold</i>"]
        ML["ML Pipelines<br/><i>Feature Store + Embeddings</i>"]
        AIR --> CSV
        AIR --> DBT
        AIR --> ML
    end

    subgraph Output ["Destinations"]
        MONGO["MongoDB<br/>(Profiles)"]
        BQ["BigQuery<br/>(Analytics)"]
    end

    PB1 --> MONGO
    PB1 --> BQ
    DBT --> BQ
    CSV --> BQ
```

---

## 5. Identity Resolution (Slide 7)

```mermaid
graph TB
    subgraph Identifiers ["Incoming Identifiers"]
        E1["session_id<br/><i>Website</i>"]
        E2["email<br/><i>Form / CRM / App</i>"]
        E3["device_id<br/><i>Mobile App</i>"]
        E4["sf_id<br/><i>Salesforce</i>"]
        E5["phone<br/><i>WhatsApp / CRM</i>"]
    end

    subgraph Phase1 ["Phase 1: Deterministic — Exact Match"]
        DET["MongoDB Indexed Lookup<br/><i>identifiers.value</i><br/>~85% of matches"]
    end

    subgraph Phase2 ["Phase 2: Probabilistic — Fuzzy"]
        PROB["Name + Location Similarity<br/><i>Confidence threshold: 0.85</i><br/>~15% of matches"]
    end

    subgraph Result ["Resolution Result"]
        MATCH["Merge Profiles<br/><i>CRM wins for contact info</i><br/><i>Most restrictive consent</i>"]
        NEW["Create New Profile"]
        REVIEW["Manual Review Queue<br/><i>Confidence 0.7 – 0.85</i>"]
    end

    E1 --> DET
    E2 --> DET
    E3 --> DET
    E4 --> DET
    E5 --> DET
    DET -->|match found| MATCH
    DET -->|no match| PROB
    PROB -->|confidence ≥ 0.85| MATCH
    PROB -->|confidence < 0.7| NEW
    PROB -->|0.7 – 0.85| REVIEW
```

---

## 6. Storage Layer (Slide 8)

```mermaid
graph TB
    EVT["Kafka Event<br/><i>Same event writes to both</i>"]

    subgraph Operational ["MongoDB — Operational Store"]
        M1["< 10ms reads"]
        M2["Flexible document model"]
        M3["Used by: Profile API,<br/>Segmentation, Triggers"]
        M4["5M+ documents"]
    end

    subgraph Analytical ["BigQuery — Analytical Store"]
        B1["Seconds to minutes"]
        B2["Structured SQL tables<br/>(partitioned + clustered)"]
        B3["Used by: Analytics team,<br/>Data Science, Reverse ETL"]
        B4["Petabyte-scale, $5/TB"]
    end

    RECON["Nightly Reconciliation<br/><i>cdp_dag.py</i><br/>Detect & heal drift"]

    EVT --> Operational
    EVT --> Analytical
    Operational <-->|compare| RECON
    Analytical <-->|compare| RECON
```

---

## 7. Medallion Architecture (Slide 9)

```mermaid
graph LR
    SRC(("Sources"))

    SRC --> G1{{"Gate 1<br/>Schema"}}
    G1 --> BZ["**Bronze**<br/>Raw formats<br/>*90 days*"]

    BZ -->|format_normalizer| G2{{"Gate 2<br/>Clean"}}
    G2 --> SV["**Silver**<br/>Unified JSON<br/>*1 year*"]

    SV -->|dbt + identity_res| G3{{"Gate 3<br/>Enrich"}}
    G3 --> GD["**Gold**<br/>Profiles + Segments<br/>*2 years*"]

    GD --> BQ["BigQuery"]
    GD --> VFS["Vertex AI"]
```

---

## 8. Reverse ETL (Slide 10)

```mermaid
graph LR
    BQ["BigQuery Gold<br/><i>Enriched profiles</i>"]
    SEG["Segmentation Engine<br/><i>Evaluate rules</i>"]
    CONSENT["Consent Check<br/><i>consent_manager.py</i>"]
    GATE4["Gate 4<br/><i>PII safety + row count</i>"]

    subgraph Targets ["Activation Targets"]
        SF["Salesforce<br/><i>Update Contact fields</i><br/><i>Create Tasks</i>"]
        TW["Twilio / WhatsApp<br/><i>Personalized messages</i>"]
        EM["Email Platform<br/><i>Segment membership</i>"]
        PUSH["Mobile Push<br/><i>Lesson reminders</i>"]
    end

    BQ --> SEG --> CONSENT
    CONSENT -->|consented| GATE4
    CONSENT -->|denied| BLOCK["Blocked + Audit logged"]
    GATE4 --> SF
    GATE4 --> TW
    GATE4 --> EM
    GATE4 --> PUSH
```

---

## 9. Segmentation & Triggers (Slide 11)

```mermaid
graph TB
    EVT["Student browses<br/>MBA page 3x"]
    KF["Kafka Event<br/><i>cdp.processed.interactions</i>"]

    subgraph Engine ["Segmentation Engine"]
        RULE["Rule Evaluation<br/><i>page_count mba ≥ 3</i><br/><i>AND status = inquiry</i><br/><i>AND consent.email = true</i>"]
        TRIGGER["Trigger:<br/><b>high_intent_mba_prospect</b>"]
    end

    subgraph Actions ["Multi-Channel Actions < 30s"]
        A1["Email<br/><i>Scholarship deadline<br/>in 14 days</i>"]
        A2["WhatsApp<br/><i>Your advisor is<br/>here to help</i>"]
        A3["Salesforce<br/><i>High-intent lead —<br/>call within 24h</i>"]
    end

    EVT --> KF --> RULE --> TRIGGER
    TRIGGER --> A1
    TRIGGER --> A2
    TRIGGER --> A3
```

---

## 10. Data Quality — 4 Gates (Slide 12)

```mermaid
graph LR
    SRC(("Raw<br/>Data"))

    SRC --> G1["GATE 1<br/><b>Ingestion</b><br/>Schema · Required fields<br/>Timestamp format"]
    G1 --> BRONZE["Bronze"]
    G1 -.->|fail| DLQ["DLQ"]

    BRONZE --> G2["GATE 2<br/><b>Bronze → Silver</b><br/>Nulls · Types<br/>Dedup · Referential"]
    G2 --> SILVER["Silver"]
    G2 -.->|fail| H1["DAG Halt"]

    SILVER --> G3["GATE 3<br/><b>Silver → Gold</b><br/>Completeness ≥ 60%<br/>Identity conf ≥ 0.85"]
    G3 --> GOLD["Gold"]
    G3 -.->|fail| H2["DAG Halt"]

    GOLD --> G4["GATE 4<br/><b>Pre-Reverse ETL</b><br/>No PII leaks<br/>Row count ±20%"]
    G4 --> OUT(("Activate<br/>Channels"))
    G4 -.->|fail| BLK["Blocked"]
```

---

## 11. GDPR & Privacy (Slide 13)

```mermaid
graph LR
    REQ(("Deletion<br/>Request"))

    REQ --> D1["MongoDB"]
    REQ --> D2["BigQuery"]
    REQ --> D3["Pinecone"]
    REQ --> D4["Vertex AI"]
    REQ --> D5["Kafka<br/>*tombstone*"]
    REQ --> D6["Salesforce"]

    D1 --> AUDIT["Audit Log"]
    D2 --> AUDIT
    D3 --> AUDIT
    D4 --> AUDIT
    D5 --> AUDIT
    D6 --> AUDIT

    AUDIT --> VERIFY["Verify<br/>*0 results in all stores*"]

    subgraph PII ["PII Protection"]
        P1["Field Encryption<br/>*GCP KMS*"]
        P2["Tokenization<br/>*No raw PII in analytics*"]
        P3["Log Redaction<br/>*Auto-scrub PII*"]
    end
```

---

## 12. AI/ML Layer (Slide 14)

```mermaid
graph LR
    BQ(("BigQuery<br/>Gold"))

    BQ --> AIR["Airflow<br/>*Nightly batch*"]
    BQ --> STREAM["Stream<br/>*Real-time*"]

    AIR --> VFS["**Vertex AI**<br/>Feature Store<br/>*< 10ms serving*"]
    STREAM --> VFS

    AIR --> EMB["Embedding<br/>Generator<br/>*textembedding-gecko*"]
    EMB --> PC["**Pinecone**<br/>Vector Search<br/>*< 50ms queries*"]

    VFS --> USE1["Churn Prediction<br/>Next-Best-Action<br/>Enrollment Probability"]
    PC --> USE2["Students Like You<br/>Course Recs<br/>Auto-Reply"]
```

---

## 13. Observability (Slide 15)

```mermaid
graph LR
    subgraph Logs ["Logs"]
        L1["Cloud Logging<br/>*Structured JSON*"]
        L2["Correlation IDs"]
        L3["PII Redacted"]
    end

    subgraph Metrics ["Metrics"]
        M1["Prometheus + Grafana"]
        M2["kafka_lag · api_p99<br/>freshness · dlq_count"]
    end

    subgraph Traces ["Traces"]
        T1["OpenTelemetry"]
        T2["End-to-end latency"]
    end

    Logs --> CRIT["**CRITICAL**<br/>PagerDuty<br/>*Lag > 10K, p99 > 500ms*"]
    Metrics --> CRIT
    Traces --> CRIT

    Logs --> WARN["**WARNING**<br/>Slack<br/>*Freshness > 1hr, cost > 80%*"]
    Metrics --> WARN

    Metrics --> INFO["**INFO**<br/>Dashboard<br/>*Daily counts, trends*"]
```
