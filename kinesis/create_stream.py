"""
Script: Create AWS Kinesis Data Stream for Sales Pipeline
Run this once to set up your stream before deploying Lambda.
"""

import boto3
import time

STREAM_NAME  = 'sales-stream'
SHARD_COUNT  = 2          # Increase for higher throughput
REGION       = 'us-east-1'

client = boto3.client('kinesis', region_name=REGION)


def create_stream():
    print(f"Creating Kinesis stream: '{STREAM_NAME}' with {SHARD_COUNT} shard(s)...")

    try:
        client.create_stream(
            StreamName=STREAM_NAME,
            ShardCount=SHARD_COUNT
        )
    except client.exceptions.ResourceInUseException:
        print(f"Stream '{STREAM_NAME}' already exists. Skipping creation.")
        return

    # Wait until stream is ACTIVE
    print("Waiting for stream to become ACTIVE...")
    while True:
        response = client.describe_stream_summary(StreamName=STREAM_NAME)
        status   = response['StreamDescriptionSummary']['StreamStatus']
        print(f"  Status: {status}")
        if status == 'ACTIVE':
            print("Stream is ACTIVE and ready!")
            break
        time.sleep(5)

    # Print stream ARN
    response   = client.describe_stream_summary(StreamName=STREAM_NAME)
    stream_arn = response['StreamDescriptionSummary']['StreamARN']
    print(f"\nStream ARN: {stream_arn}")
    print("Use this ARN when creating the Lambda event source mapping.")


if __name__ == '__main__':
    create_stream()
