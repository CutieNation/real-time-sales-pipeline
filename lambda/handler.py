"""
Lambda Function: Real-Time Sales Data Processor
Triggered by: AWS Kinesis Data Streams
Action: Validates, transforms, and loads POS events into Redshift
"""

import json
import base64
import boto3
import psycopg2
import os
import logging
from datetime import datetime

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables (set in Lambda console or Terraform)
REDSHIFT_HOST     = os.environ['REDSHIFT_HOST']
REDSHIFT_PORT     = os.environ.get('REDSHIFT_PORT', '5439')
REDSHIFT_DB       = os.environ['REDSHIFT_DB']
REDSHIFT_USER     = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWORD = os.environ['REDSHIFT_PASSWORD']
REDSHIFT_TABLE    = os.environ.get('REDSHIFT_TABLE', 'sales_transactions')


def get_redshift_connection():
    """Create and return a Redshift connection."""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=int(REDSHIFT_PORT),
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )


def decode_kinesis_record(record):
    """Decode a base64-encoded Kinesis record into a Python dict."""
    raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw_data)


def validate_record(data):
    """
    Validate required fields in each POS event.
    Returns True if valid, False otherwise.
    """
    required_fields = [
        'transaction_id', 'store_id', 'product_id',
        'product_name', 'quantity', 'unit_price',
        'total_amount', 'payment_method', 'timestamp'
    ]
    for field in required_fields:
        if field not in data:
            logger.warning(f"Missing field '{field}' in record: {data}")
            return False
    return True


def transform_record(data):
    """
    Transform and enrich the raw POS event.
    - Calculates discount if applicable
    - Adds ingestion timestamp
    - Normalises payment method to uppercase
    """
    transformed = {
        'transaction_id' : data['transaction_id'],
        'store_id'        : data['store_id'],
        'product_id'      : data['product_id'],
        'product_name'    : data['product_name'].strip().title(),
        'category'        : data.get('category', 'UNKNOWN'),
        'quantity'        : int(data['quantity']),
        'unit_price'      : float(data['unit_price']),
        'total_amount'    : float(data['total_amount']),
        'discount_amount' : float(data.get('discount_amount', 0.0)),
        'payment_method'  : data['payment_method'].upper(),
        'customer_id'     : data.get('customer_id', 'GUEST'),
        'region'          : data.get('region', 'UNKNOWN'),
        'event_timestamp' : data['timestamp'],
        'ingested_at'     : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }
    return transformed


def insert_to_redshift(conn, records):
    """
    Bulk insert transformed records into Redshift.
    Uses executemany for efficiency.
    """
    insert_sql = f"""
        INSERT INTO {REDSHIFT_TABLE} (
            transaction_id, store_id, product_id, product_name,
            category, quantity, unit_price, total_amount,
            discount_amount, payment_method, customer_id,
            region, event_timestamp, ingested_at
        ) VALUES (
            %(transaction_id)s, %(store_id)s, %(product_id)s, %(product_name)s,
            %(category)s, %(quantity)s, %(unit_price)s, %(total_amount)s,
            %(discount_amount)s, %(payment_method)s, %(customer_id)s,
            %(region)s, %(event_timestamp)s, %(ingested_at)s
        )
    """
    cursor = conn.cursor()
    cursor.executemany(insert_sql, records)
    conn.commit()
    cursor.close()
    logger.info(f"Successfully inserted {len(records)} records into Redshift.")


def lambda_handler(event, context):
    """
    Main Lambda entry point.
    Processes a batch of Kinesis records.
    """
    logger.info(f"Received {len(event['Records'])} Kinesis records.")

    valid_records   = []
    invalid_count   = 0

    # Step 1: Decode and validate each record
    for record in event['Records']:
        try:
            data = decode_kinesis_record(record)
            if validate_record(data):
                transformed = transform_record(data)
                valid_records.append(transformed)
            else:
                invalid_count += 1
        except Exception as e:
            logger.error(f"Error decoding record: {e}")
            invalid_count += 1

    logger.info(f"Valid: {len(valid_records)} | Invalid/Skipped: {invalid_count}")

    # Step 2: Insert valid records to Redshift
    if valid_records:
        try:
            conn = get_redshift_connection()
            insert_to_redshift(conn, valid_records)
            conn.close()
        except Exception as e:
            logger.error(f"Redshift insertion failed: {e}")
            raise e

    return {
        'statusCode'     : 200,
        'processed'      : len(valid_records),
        'skipped'        : invalid_count
    }
