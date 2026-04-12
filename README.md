**HR Analytics Pipeline with Gemini AI, GCP & BigQuery**

This project demonstrates an end-to-end data engineering + AI enrichment pipeline that generates synthetic employee data, analyzes feedback using Google Gemini AI, and loads enriched insights into Google Cloud Platform (GCS + BigQuery).🚀 Features

 **Features**

1. Generate realistic synthetic employee data using Faker
2. Create rule-based employee feedback
3. Enrich feedback using Gemini AI (LLM) for:
    3.a. Sentiment analysis (positive / neutral / negative)
    3.b. Categorization (Compensation, Management, etc.)
4. Batch processing with retry handling
5. Upload processed data to Google Cloud Storage (GCS)
6. Load structured data into BigQuery for analytics

**Output**

1. Enriched Dataset Includes:
   Employee details
   Feedback text
   AI-generated:
   Category
   Sentiment

2. GCS Output

gs://<bucket-name>/enriched/employee_enriched.csv

4. BigQuery Table
   
 project-id.hr_analytics.employee_enriched

