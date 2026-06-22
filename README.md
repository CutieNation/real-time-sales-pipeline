# 🛒 Real-Time Sales Data Pipeline
### AWS Kinesis → Lambda → Redshift → QuickSight

A production-style real-time streaming pipeline that ingests point-of-sale (POS) data, processes it through AWS Lambda, stores it in Amazon Redshift, and visualizes it via Amazon QuickSight dashboards.

---

## 📐 Architecture

```
POS Systems / Data Generator
        │
        ▼
 AWS Kinesis Data Streams
  (real-time ingestion)
        │
        ▼
  AWS Lambda Function
  (transform & validate)
        │
        ▼
  Amazon Redshift
  (data warehouse)
        │
        ▼
  Amazon QuickSight
  (live dashboards)
```

---

## 📁 Project Structure

```
sales-pipeline/
├── README.md
├── lambda/
│   ├── handler.py              # Main Lambda function
│   └── requirements.txt        # Python dependencies
├── kinesis/
│   └── create_stream.py        # Script to create Kinesis stream
├── redshift/
│   ├── create_tables.sql       # Redshift DDL
│   └── queries.sql             # Sample analytical queries
├── data_generator/
│   └── pos_simulator.py        # Simulates POS events → Kinesis
├── terraform/
│   ├── main.tf                 # Infrastructure as Code
│   ├── variables.tf
│   └── outputs.tf
├── quicksight/
│   └── dashboard_setup.md      # QuickSight setup guide
└── docs/
    └── architecture.md         # Detailed architecture notes
```

---

## 🚀 Quick Start

### Prerequisites
- AWS Account with appropriate IAM permissions
- Python 3.9+
- AWS CLI configured (`aws configure`)
- Terraform (optional, for IaC setup)

### Step 1 — Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/sales-pipeline.git
cd sales-pipeline
```

### Step 2 — Install dependencies
```bash
pip install -r lambda/requirements.txt
pip install boto3  # for data generator
```

### Step 3 — Set up AWS infrastructure
```bash
# Create Kinesis Stream
python kinesis/create_stream.py

# Create Redshift tables
# Run redshift/create_tables.sql in your Redshift query editor
```

### Step 4 — Deploy Lambda
```bash
cd lambda
zip function.zip handler.py
aws lambda create-function \
  --function-name sales-pipeline-processor \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-kinesis-role \
  --handler handler.lambda_handler \
  --zip-file fileb://function.zip
```

### Step 5 — Add Kinesis trigger to Lambda
```bash
aws lambda create-event-source-mapping \
  --function-name sales-pipeline-processor \
  --event-source-arn arn:aws:kinesis:us-east-1:YOUR_ACCOUNT_ID:stream/sales-stream \
  --starting-position LATEST \
  --batch-size 100
```

### Step 6 — Run the POS data simulator
```bash
python data_generator/pos_simulator.py
```

### Step 7 — Visualize in QuickSight
Follow `quicksight/dashboard_setup.md` for dashboard creation steps.

---

## 🛠️ Tech Stack

| Layer | Service |
|---|---|
| Streaming Ingestion | AWS Kinesis Data Streams |
| Processing | AWS Lambda (Python 3.9) |
| Storage / Warehouse | Amazon Redshift |
| Visualization | Amazon QuickSight |
| IaC | Terraform |
| Language | Python |

---

## 📊 Sample Dashboard Metrics
- Total sales by hour / day
- Top selling products
- Revenue by store/region
- Transaction volume trends

---

## 👤 Author
Ravi Kumar Reddy  
[LinkedIn](https://linkedin.com/in/ravikumarreddy) | [GitHub](https://github.com/ravikumarreddy)
