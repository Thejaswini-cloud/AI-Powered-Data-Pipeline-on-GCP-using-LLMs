# full_pipeline_free_tier.py
import pandas as pd
from faker import Faker
import random
import json
import time
from io import BytesIO, StringIO

import google.generativeai as genai
from google.cloud import storage, bigquery

# ── Config ──────────────────────────────────────────────
GEMINI_API_KEY = "AIzaSyAx59-sbNQulfotDMaB1atYZ9gI4MS1-eo"   # <-- paste here
GCP_PROJECT    = "custom-vigil-346206"
BUCKET_NAME    = "ai-sbucket1"
BQ_DATASET     = "hr_analytics"
BQ_TABLE       = "employee_enriched"

# ── Gemini (free, no billing needed) ────────────────────
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

# ── 1. Generate fake data ────────────────────────────────
fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_employee(emp_id):
    return {
        "employee_id": emp_id,
        "first_name":  fake.first_name(),
        "last_name":   fake.last_name(),
        "department":  random.choice(["Engineering","HR","Finance","Sales","Marketing","Operations"]),
        "salary":      random.randint(50000, 180000),
        "hire_date":   str(fake.date_between(start_date="-10y", end_date="today")),
        "job_title":   fake.job(),
        "email":       fake.email(),
    }

def generate_feedback(row):
    if row["salary"] < 60000:
        return "I feel my salary is not competitive"
    elif row["department"] == "Engineering":
        return "Work is interesting but sometimes stressful"
    elif row["department"] == "HR":
        return "Work environment is good but growth is slow"
    elif row["department"] == "Sales":
        return "Targets are high and pressure is intense"
    elif row["salary"] > 120000:
        return "I am very satisfied with my compensation and role"
    else:
        return "Overall I am happy with my job"

df = pd.DataFrame([generate_employee(i+1) for i in range(100)])
df["feedback_text"] = df.apply(generate_feedback, axis=1)
print(f"Generated {len(df)} employee records")

# ── 2. Enrich with Gemini ────────────────────────────────
def enrich_batch(texts):
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(texts))
    prompt = f"""You are an HR analytics AI.
For each numbered feedback, return a JSON array. Each item must have:
  "category": one of [Compensation, Management, Work Environment, Career Growth, Work-Life Balance, Other]
  "sentiment": one of [positive, neutral, negative]

Return ONLY the raw JSON array, no markdown, no explanation.

Feedbacks:
{numbered}"""
    response = model.generate_content(prompt)
    raw = response.text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    return json.loads(raw)

results = []
batch_size = 10

for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size]
    try:
        enriched = enrich_batch(batch["feedback_text"].tolist())
        for idx, row in enumerate(batch.to_dict(orient="records")):
            row["category"] = enriched[idx].get("category")
            row["sentiment"] = enriched[idx].get("sentiment")
            results.append(row)
        print(f"Batch {i//batch_size+1}/{len(df)//batch_size} done ✓")
    except Exception as e:
        print(f"Batch failed: {e}")
        for row in batch.to_dict(orient="records"):
            row["category"] = None
            row["sentiment"] = None
            results.append(row)
    time.sleep(1)   # stay within free rate limits

enriched_df = pd.DataFrame(results)
print(enriched_df[["department","salary","category","sentiment"]].head(10))

# ── 3. Upload enriched CSV to GCS ───────────────────────
def upload_to_gcs(df, bucket, blob_name):
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    storage.Client().bucket(bucket).blob(blob_name).upload_from_file(
        buf, content_type="text/csv"
    )
    print(f"Uploaded to gs://{bucket}/{blob_name}")

upload_to_gcs(enriched_df, BUCKET_NAME, "enriched/employee_enriched.csv")

# ── 4. Load into BigQuery ────────────────────────────────
def load_to_bigquery(df, project, dataset, table):
    client = bigquery.Client(project=project)

    # Create dataset if needed
    ds_ref = f"{project}.{dataset}"
    try:
        client.get_dataset(ds_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(ds_ref))
        print(f"Created dataset {dataset}")

    table_ref = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    # Load directly from DataFrame (no need to re-read from GCS)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {client.get_table(table_ref).num_rows} rows into {table_ref}")

load_to_bigquery(enriched_df, GCP_PROJECT, BQ_DATASET, BQ_TABLE)
print("Pipeline complete!")