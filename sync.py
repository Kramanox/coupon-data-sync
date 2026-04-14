#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 Coupon Data Sync Script - TEST VERSION (Algeria Only)
✅ يجلب الكوبونات والمتاجر الخاصة بالجزائر فقط
✅ للتحقق من أن الاتصال بـ API و R2 يعمل قبل التشغيل الكامل
"""

import os
import time
import json
import requests
import boto3
from botocore.config import Config

# ─────────────────────────────────────────────
# 🇩🇩 دولة التجربة: الجزائر
# ─────────────────────────────────────────────
TEST_COUNTRY = "DZ"

# ─────────────────────────────────────────────
# 📥 دالة جلب جميع الصفحات مع pagination
# ─────────────────────────────────────────────
def fetch_all_pages(base_endpoint, params, headers, delay_seconds=12):
    results = []
    offset = 0
    page_count = 0
    
    while True:
        params["offset"] = offset
        print(f"📥 Page {page_count + 1} | Offset: {offset}")
        
        try:
            response = requests.get(
                base_endpoint, 
                params=params, 
                headers=headers, 
                timeout=30
            )
            
            if response.status_code == 400:
                print(f"⚠️  Bad Request (400) - Response: {response.text[:300]}")
                break
                
            response.raise_for_status()
            data = response.json()
            
            page_results = data.get("results", [])
            results.extend(page_results)
            
            print(f"✅ Got {len(page_results)} items | Total: {len(results)}")
            
            if not data.get("next") or len(page_results) == 0:
                print("🎉 No more pages!")
                break
            
            offset += len(page_results)
            page_count += 1
            
            print(f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            if page_count == 0:
                raise
            break
    
    return results

# ─────────────────────────────────────────────
# ☁️ دالة رفع البيانات إلى Cloudflare R2
# ─────────────────────────────────────────────
def upload_to_r2(data_list, filename, env):
    print(f"☁️ Uploading {filename} to R2...")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT_URL"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4")
    )
    
    bucket = env["R2_BUCKET_NAME"]
    json_data = json.dumps(data_list, ensure_ascii=False, separators=(',', ':'))
    
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    
    print(f"✅ Uploaded {filename} ({len(data_list)} items) to {bucket}")

# ─────────────────────────────────────────────
# 🚀 الدالة الرئيسية - نسخة التجربة
# ─────────────────────────────────────────────
def main():
    base_url = os.environ["API_BASE_URL"].rstrip('/')
    api_token = os.environ["API_TOKEN"]
    source_id = os.environ.get("SOURCE_ID", "")
    
    env = {
        "R2_ENDPOINT_URL": os.environ["R2_ENDPOINT_URL"],
        "R2_ACCESS_KEY_ID": os.environ["R2_ACCESS_KEY_ID"],
        "R2_SECRET_ACCESS_KEY": os.environ["R2_SECRET_ACCESS_KEY"],
        "R2_BUCKET_NAME": os.environ["R2_BUCKET_NAME"]
    }
    
    headers = {
        "Authorization": api_token,
        "Accept": "application/json"
    }
    
    print("=" * 70)
    print(f"🚀 TEST SYNC - Country: {TEST_COUNTRY} 🇩🇿")
    print(f"📡 API Base: {base_url}")
    print(f"🔑 Source ID: {source_id[:8] if source_id else 'N/A'}...")
    print("=" * 70)
    
    # ─────────────────────────────────────────
    # 1️⃣ جلب الكوبونات للجزائر فقط (geos* مطلوب)
    # ─────────────────────────────────────────
    print(f"\n🟢 Step 1: Fetching Coupons for {TEST_COUNTRY}...")
    
    coupon_params = {
        "limit": 100,
        "is_active": "true",
        "source_id": source_id,
        "geos": TEST_COUNTRY  # ✅ مطلوب حسب Swagger
    }
    
    try:
        coupons = fetch_all_pages(
            f"{base_url}/public_api/v1/coupons",
            coupon_params,
            headers,
            delay_seconds=12  # تأخير آمن
        )
        print(f"\n✅ Retrieved {len(coupons)} coupons for {TEST_COUNTRY}")
        upload_to_r2(coupons, "coupons.json", env)
    except Exception as e:
        print(f"❌ Error fetching coupons: {e}")
        coupons = []
    
    # ─────────────────────────────────────────
    # 2️⃣ جلب التجار المرتبطين بالجزائر (country اختياري)
    # ─────────────────────────────────────────
    print(f"\n🟢 Step 2: Fetching Merchants for {TEST_COUNTRY}...")
    
    merchant_params = {
        "limit": 100,
        "status": "active",
        "source_id": source_id,
        "country": TEST_COUNTRY  # ✅ فلتر اختياري للتجار
    }
    
    try:
        merchants = fetch_all_pages(
            f"{base_url}/public_api/v1/merchants",
            merchant_params,
            headers,
            delay_seconds=12
        )
        print(f"✅ Retrieved {len(merchants)} merchants for {TEST_COUNTRY}")
        upload_to_r2(merchants, "merchants.json", env)
    except Exception as e:
        print(f"❌ Error fetching merchants: {e}")
        merchants = []
    
    # ─────────────────────────────────────────
    # 📊 التقرير النهائي
    # ─────────────────────────────────────────
    print("\n" + "=" * 70)
    if coupons or merchants:
        print("🎉 TEST SYNC COMPLETED SUCCESSFULLY! ✅")
        print(f"📦 coupons.json   : {len(coupons)} coupons for {TEST_COUNTRY}")
        print(f"📦 merchants.json : {len(merchants)} merchants for {TEST_COUNTRY}")
        print(f"🪣 Bucket        : {env['R2_BUCKET_NAME']}")
        print("\n🔍 Next: Check your Worker can read these files from R2!")
    else:
        print("⚠️  No data retrieved - check API credentials or country code")
    print("=" * 70)

# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()
