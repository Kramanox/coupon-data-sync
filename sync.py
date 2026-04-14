import os
import time
import json
import requests
import boto3
from botocore.config import Config

def fetch_all_pages(base_endpoint, params, headers):
    results = []
    offset = 0
    while True:
        params["offset"] = offset
        response = requests.get(base_endpoint, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        page_results = data.get("results", [])
        results.extend(page_results)

        print(f"📥 Fetched page ending at offset {offset} ({len(page_results)} items)")

        if not data.get("next"):
            break

        offset += len(page_results)
        print("⏳ Waiting 12 seconds to avoid API rate limits...")
        time.sleep(12)

    return results

def upload_to_r2(data_list, filename):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4")
    )
    bucket = os.environ["R2_BUCKET_NAME"]
    json_data = json.dumps(data_list, ensure_ascii=False, separators=(',', ':'))
    s3.put_object(Bucket=bucket, Key=filename, Body=json_data.encode('utf-8'), ContentType="application/json")
    print(f"☁️ Uploaded {filename} ({len(data_list)} items) to R2 Bucket: {bucket}")

def main():
    base_url = os.environ["API_BASE_URL"].rstrip('/')
    api_token = os.environ["API_TOKEN"]
    source_id = os.environ.get("SOURCE_ID", "")

    headers = {
        "Authorization": api_token,
        "Accept": "application/json"
    }

    # 1. Fetch Coupons
    print("🟢 Starting Coupons sync...")
    coupon_params = {"limit": 100, "is_active": "true", "source_id": source_id}
    coupons = fetch_all_pages(f"{base_url}/public_api/v1/coupons", coupon_params, headers)
    upload_to_r2(coupons, "coupons.json")

    # 2. Fetch Merchants
    print("🟢 Starting Merchants sync...")
    merchant_params = {"limit": 100, "status": "active", "source_id": source_id}
    merchants = fetch_all_pages(f"{base_url}/public_api/v1/merchants", merchant_params, headers)
    upload_to_r2(merchants, "merchants.json")

    print("🎉 Sync completed successfully!")

if __name__ == "__main__":
    main()
