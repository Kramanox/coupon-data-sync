import os
import time
import json
import requests
import boto3
from botocore.config import Config

def fetch_all_pages(base_endpoint, params, headers):
    """جلب جميع الصفحات من API مع pagination"""
    results = []
    offset = 0
    page_count = 0
    
    while True:
        params["offset"] = offset
        print(f"📥 Fetching page {page_count + 1} (offset: {offset})...")
        
        try:
            response = requests.get(base_endpoint, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            page_results = data.get("results", [])
            results.extend(page_results)
            
            print(f"✅ Retrieved {len(page_results)} items (Total: {len(results)})")
            
            # التحقق من وجود صفحة تالية
            if not data.get("next") or len(page_results) == 0:
                print("🎉 No more pages to fetch!")
                break
            
            offset += len(page_results)
            page_count += 1
            
            # انتظار 12 ثانية بين كل طلب لتجنب الحظر
            print("⏳ Waiting 12 seconds before next request...")
            time.sleep(12)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching page: {e}")
            raise
    
    return results

def upload_to_r2(data_list, filename):
    """رفع البيانات إلى Cloudflare R2"""
    print(f"☁️ Uploading {filename} to R2...")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4")
    )
    
    bucket = os.environ["R2_BUCKET_NAME"]
    
    # تحويل البيانات إلى JSON مضغوط
    json_data = json.dumps(data_list, ensure_ascii=False, separators=(',', ':'))
    
    # رفع الملف إلى R2
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    
    print(f"✅ Successfully uploaded {filename} ({len(data_list)} items) to R2 Bucket: {bucket}")

def main():
    # جلب المتغيرات من البيئة
    base_url = os.environ["API_BASE_URL"].rstrip('/')
    api_token = os.environ["API_TOKEN"]
    source_id = os.environ.get("SOURCE_ID", "")
    
    # إعداد الهيدرز
    headers = {
        "Authorization": api_token,
        "Accept": "application/json"
    }
    
    print("=" * 60)
    print("🚀 Starting API Data Sync to Cloudflare R2")
    print("=" * 60)
    
    # 1. جلب الكوبونات
    print("\n🟢 Step 1: Fetching Coupons...")
    coupon_params = {
        "limit": 100,
        "is_active": "true",
        "source_id": source_id
    }
    coupons = fetch_all_pages(f"{base_url}/public_api/v1/coupons", coupon_params, headers)
    upload_to_r2(coupons, "coupons.json")
    
    # 2. جلب التجار
    print("\n🟢 Step 2: Fetching Merchants...")
    merchant_params = {
        "limit": 100,
        "status": "active",
        "source_id": source_id
    }
    merchants = fetch_all_pages(f"{base_url}/public_api/v1/merchants", merchant_params, headers)
    upload_to_r2(merchants, "merchants.json")
    
    print("\n" + "=" * 60)
    print("🎉 Sync completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
