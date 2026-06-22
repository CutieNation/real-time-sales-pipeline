"""
Script: POS (Point-of-Sale) Data Simulator
Simulates real-time sales transactions and sends them to Kinesis.
Run this to test your pipeline end-to-end.

Usage:
    python pos_simulator.py                   # runs indefinitely
    python pos_simulator.py --count 500       # sends 500 records and stops
    python pos_simulator.py --delay 0.5       # 0.5 second delay between records
"""

import boto3
import json
import uuid
import random
import time
import argparse
from datetime import datetime

# ── Configuration ────────────────────────────────────────────
STREAM_NAME  = 'sales-stream'
REGION       = 'us-east-1'

# ── Sample Data ──────────────────────────────────────────────
STORES = [
    {'store_id': 'STR001', 'region': 'South'},
    {'store_id': 'STR002', 'region': 'North'},
    {'store_id': 'STR003', 'region': 'East'},
    {'store_id': 'STR004', 'region': 'West'},
    {'store_id': 'STR005', 'region': 'Central'},
]

PRODUCTS = [
    {'product_id': 'PRD001', 'product_name': 'Wireless Earbuds',    'category': 'Electronics', 'unit_price': 2499.00},
    {'product_id': 'PRD002', 'product_name': 'Running Shoes',        'category': 'Footwear',    'unit_price': 3299.00},
    {'product_id': 'PRD003', 'product_name': 'Coffee Maker',         'category': 'Appliances',  'unit_price': 1899.00},
    {'product_id': 'PRD004', 'product_name': 'Yoga Mat',             'category': 'Sports',      'unit_price': 799.00},
    {'product_id': 'PRD005', 'product_name': 'Laptop Stand',         'category': 'Electronics', 'unit_price': 1299.00},
    {'product_id': 'PRD006', 'product_name': 'Protein Powder',       'category': 'Health',      'unit_price': 1499.00},
    {'product_id': 'PRD007', 'product_name': 'Smart Watch',          'category': 'Electronics', 'unit_price': 5999.00},
    {'product_id': 'PRD008', 'product_name': 'Water Bottle',         'category': 'Sports',      'unit_price': 499.00},
    {'product_id': 'PRD009', 'product_name': 'Desk Lamp',            'category': 'Home',        'unit_price': 899.00},
    {'product_id': 'PRD010', 'product_name': 'Bluetooth Speaker',    'category': 'Electronics', 'unit_price': 3499.00},
]

PAYMENT_METHODS = ['CREDIT_CARD', 'DEBIT_CARD', 'UPI', 'CASH', 'NET_BANKING']


def generate_transaction():
    """Generate a single realistic POS transaction."""
    store   = random.choice(STORES)
    product = random.choice(PRODUCTS)
    qty     = random.randint(1, 5)
    price   = product['unit_price']

    # Random discount (30% chance)
    discount = round(price * qty * random.uniform(0.05, 0.20), 2) \
               if random.random() < 0.3 else 0.0

    total = round((price * qty) - discount, 2)

    return {
        'transaction_id'  : str(uuid.uuid4()),
        'store_id'        : store['store_id'],
        'region'          : store['region'],
        'product_id'      : product['product_id'],
        'product_name'    : product['product_name'],
        'category'        : product['category'],
        'quantity'        : qty,
        'unit_price'      : price,
        'total_amount'    : total,
        'discount_amount' : discount,
        'payment_method'  : random.choice(PAYMENT_METHODS),
        'customer_id'     : f"CUST{random.randint(1000, 9999)}",
        'timestamp'       : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }


def send_to_kinesis(client, record):
    """Send a single transaction record to Kinesis."""
    response = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(record),
        PartitionKey=record['store_id']   # partition by store
    )
    return response['SequenceNumber']


def main(count=None, delay=1.0):
    client    = boto3.client('kinesis', region_name=REGION)
    sent      = 0
    print(f"Starting POS simulator → Kinesis stream: '{STREAM_NAME}'")
    print(f"Delay: {delay}s | Count: {'unlimited' if count is None else count}\n")

    try:
        while True:
            record   = generate_transaction()
            seq_no   = send_to_kinesis(client, record)
            sent    += 1

            print(
                f"[{sent}] Sent | "
                f"Store: {record['store_id']} | "
                f"Product: {record['product_name']:<25} | "
                f"Amount: ₹{record['total_amount']:>8.2f} | "
                f"Payment: {record['payment_method']}"
            )

            if count and sent >= count:
                print(f"\nDone. Sent {sent} records.")
                break

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nStopped. Total records sent: {sent}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='POS Data Simulator for Kinesis')
    parser.add_argument('--count', type=int, default=None,
                        help='Number of records to send (default: unlimited)')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay in seconds between records (default: 1.0)')
    args = parser.parse_args()
    main(count=args.count, delay=args.delay)

