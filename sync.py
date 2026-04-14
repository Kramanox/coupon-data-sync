#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 Coupon Data Sync Script
✅ يجلب الكوبونات من API مع دعم pagination
✅ يتعامل مع حقل geos* المطلوب حسب وثائق Swagger
✅ يزيل التكرار (كوبون واحد قد يكون في عدة دول)
✅ يرفع البيانات إلى Cloudflare R2
✅ ينتظر 12 ثانية بين الطلبات لتجنب الحظر
"""

import os
import time
import json
import requests
import boto3
from botocore.config import Config

# ─────────────────────────────────────────────
# 🌍 قائمة جميع الدول (من ملف countries.js)
# ─────────────────────────────────────────────
ALL_COUNTRIES = [
    "DZ"
]

# ─────────────────────────────────────────────
# 📥 دالة جلب جميع الصفحات مع pagination
# ─────────────────────────────────────────────
def fetch_all_pages(base_endpoint, params, headers, delay_seconds=12):
    """
    يجلب جميع النتائج من API مع التعامل التلقائي مع pagination
    """
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
                print(f"⚠️  Bad Request (400) - Response: {response.text[:200]}")
                break
                
            response.raise_for_status()
            data = response.json()
            
            page_results = data.get("results", [])
            results.extend(page_results)
            
            print(f"✅ Got {len(page_results)} items | Total: {len(results)}")
            
            # التحقق من وجود صفحة تالية
            if not data.get("next") or len(page_results) == 0:
                print("🎉 No more pages!")
                break
            
            offset += len(page_results)
            page_count += 1
            
            # ✅ الانتظار لتجنب الحظر
            print(f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            if page_count == 0:
                raise  # إذا فشل من أول صفحة، نوقف السكربت
            break  # إذا فشل بعد نجاح بعض الصفحات، نكمل بما جمعنا
    
    return results

# ─────────────────────────────────────────────
# ☁️ دالة رفع البيانات إلى Cloudflare R2
# ─────────────────────────────────────────────
def upload_to_r2(data_list, filename, env):
    """
    يرفع قائمة البيانات إلى R2 كملف JSON مضغوط
    """
    print(f"☁️ Uploading {filename} to R2...")
    
    s3 = boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT_URL"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4")
    )
    
    bucket = env["R2_BUCKET_NAME"]
    
    # تحويل إلى JSON مضغوط (بدون مسافات لتقليل الحجم)
    json_data = json.dumps(data_list, ensure_ascii=False, separators=(',', ':'))
    
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    
    print(f"✅ Uploaded {filename} ({len(data_list)} items) to {bucket}")

# ─────────────────────────────────────────────
# 🧹 دالة إزالة التكرار من الكوبونات
# ─────────────────────────────────────────────
def deduplicate_coupons(coupons_list):
    """
    يزيل الكوبونات المكررة بناءً على uuid
    (لأن الكوبون الواحد قد يظهر في عدة دول)
    """
    seen_uuids = set()
    unique_coupons = []
    
    for coupon in coupons_list:
        uuid = coupon.get("uuid")
        if uuid and uuid not in seen_uuids:
            seen_uuids.add(uuid)
            unique_coupons.append(coupon)
    
    removed = len(coupons_list) - len(unique_coupons)
    if removed > 0:
        print(f"🧹 Removed {removed} duplicate coupons")
    
    return unique_coupons

# ─────────────────────────────────────────────
# 🚀 الدالة الرئيسية
# ─────────────────────────────────────────────
def main():
    # جلب المتغيرات من البيئة (GitHub Secrets)
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
    print("🚀 Starting Coupon Data Sync to Cloudflare R2")
    print(f"📡 API Base: {base_url}")
    print(f"🔑 Source ID: {source_id[:8]}...")
    print("=" * 70)
    
    # ─────────────────────────────────────────
    # 1️⃣ جلب الكوبونات (دولة دولة لأن geos* مطلوب)
    # ─────────────────────────────────────────
    print("\n🟢 Step 1: Fetching Coupons (country by country)...")
    all_coupons_raw = []
    
    for i, country in enumerate(ALL_COUNTRIES, 1):
        print(f"\n🌍 [{i}/{len(ALL_COUNTRIES)}] Fetching: {country}")
        
        params = {
            "limit": 100,
            "is_active": "true",
            "source_id": source_id,
            "geos": country  # ✅ مطلوب حسب Swagger
        }
        
        try:
            country_coupons = fetch_all_pages(
                f"{base_url}/public_api/v1/coupons",
                params,
                headers,
                delay_seconds=2  # تأخير قصير بين صفحات نفس الدولة
            )
            all_coupons_raw.extend(country_coupons)
            print(f"✅ {country}: {len(country_coupons)} coupons")
        except Exception as e:
            print(f"⚠️  Skipped {country}: {e}")
        
        # ✅ تأخير أطول بين الدول لتجنب الحظر
        if i % 10 == 0:  # كل 10 دول
            print("⏳ Long break (12s)...")
            time.sleep(12)
    
    # إزالة التكرار
    print(f"\n📊 Before dedup: {len(all_coupons_raw)} coupons")
    all_coupons = deduplicate_coupons(all_coupons_raw)
    print(f"📊 After dedup: {len(all_coupons)} unique coupons")
    
    # رفع الكوبونات إلى R2
    upload_to_r2(all_coupons, "coupons.json", env)
    
    # ─────────────────────────────────────────
    # 2️⃣ جلب التجار (مرة واحدة - country اختياري)
    # ─────────────────────────────────────────
    print("\n🟢 Step 2: Fetching Merchants...")
    
    merchant_params = {
        "limit": 100,
        "status": "active",
        "source_id": source_id
        # country اختياري، نتركه فارغاً لجلب الكل
    }
    
    try:
        all_merchants = fetch_all_pages(
            f"{base_url}/public_api/v1/merchants",
            merchant_params,
            headers,
            delay_seconds=12
        )
        print(f"✅ Retrieved {len(all_merchants)} merchants")
        upload_to_r2(all_merchants, "merchants.json", env)
    except Exception as e:
        print(f"❌ Error fetching merchants: {e}")
        all_merchants = []
    
    # ─────────────────────────────────────────
    # 📊 التقرير النهائي
    # ─────────────────────────────────────────
    print("\n" + "=" * 70)
    print("🎉 Sync Completed Successfully!")
    print(f"📦 coupons.json   : {len(all_coupons)} unique coupons")
    print(f"📦 merchants.json : {len(all_merchants)} merchants")
    print(f"🪣 Bucket        : {env['R2_BUCKET_NAME']}")
    print("=" * 70)

# ─────────────────────────────────────────────
# نقطة الدخول
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()
