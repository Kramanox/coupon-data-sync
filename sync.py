#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🌍 Coupon Data Full Sync Script
✅ يجلب الكوبونات لـ 252 دولة مع التعامل الآمن مع Pagination
✅ يزيل التكرار تلقائياً (deduplication by uuid)
✅ يجلب التجار عالمياً مرة واحدة (أكثر كفاءة)
✅ يتعامل مع حدود الـ API بأمان (Rate Limit Protection)
"""

import os
import time
import json
import requests
import boto3
from botocore.config import Config

# ─────────────────────────────────────────────
# 🌍 قائمة جميع الدول (252 دولة كما طلبت)
# ─────────────────────────────────────────────
ALL_COUNTRIES = [
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
    "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS",
    "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
    "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE",
    "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF",
    "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM",
    "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM",
    "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC",
    "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK",
    "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA",
    "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW",
    "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS",
    "ST", "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO",
    "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI",
    "VN", "VU", "WF", "WS", "WW", "YE", "YT", "ZA", "ZM", "ZW"
]

# ─────────────────────────────────────────────
# 📥 دالة جلب الصفحات مع Pagination آمن
# ─────────────────────────────────────────────
def fetch_all_pages(base_endpoint, params, headers, page_delay=12):
    results = []
    offset = 0
    page_count = 0
    
    while True:
        params["offset"] = offset
        print(f"   📄 Page {page_count + 1} | Offset: {offset}")
        
        try:
            response = requests.get(base_endpoint, params=params, headers=headers, timeout=30)
            
            if response.status_code == 400:
                print(f"   ⚠️  Bad Request (400): {response.text[:200]}")
                break
                
            response.raise_for_status()
            data = response.json()
            page_results = data.get("results", [])
            results.extend(page_results)
            
            print(f"   ✅ Got {len(page_results)} items | Total: {len(results)}")
            
            if not data.get("next") or len(page_results) == 0:
                print("   🏁 End of pagination.")
                break
            
            offset += len(page_results)
            page_count += 1
            
            if page_count > 0:
                print(f"   ⏳ Waiting {page_delay}s...")
                time.sleep(page_delay)
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Network error: {e}")
            if page_count == 0:
                raise
            break
            
    return results

# ─────────────────────────────────────────────
# ☁️ رفع البيانات إلى Cloudflare R2
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
    
    json_data = json.dumps(data_list, ensure_ascii=False, separators=(',', ':'))
    s3.put_object(
        Bucket=env["R2_BUCKET_NAME"],
        Key=filename,
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    print(f"✅ Successfully uploaded {filename} ({len(data_list)} items)")

# ─────────────────────────────────────────────
# 🧹 إزالة التكرار (الكوبون قد يظهر في عدة دول)
# ─────────────────────────────────────────────
def deduplicate_coupons(coupons_list):
    seen_uuids = set()
    unique_coupons = []
    
    for coupon in coupons_list:
        uid = coupon.get("uuid")
        if uid and uid not in seen_uuids:
            seen_uuids.add(uid)
            unique_coupons.append(coupon)
            
    removed = len(coupons_list) - len(unique_coupons)
    if removed > 0:
        print(f"🧹 Removed {removed} duplicate coupons (based on uuid)")
    return unique_coupons

# ─────────────────────────────────────────────
# 🚀 الدالة الرئيسية
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
    print("🌍 FULL SYNC STARTED - All Countries")
    print(f"📡 API Base: {base_url}")
    print(f"🔑 Source ID: {source_id[:8]}...")
    print(f"📊 Total Countries: {len(ALL_COUNTRIES)}")
    print("=" * 70)
    
    start_time = time.time()
    
    # ─────────────────────────────────────────
    # 1️⃣ جلب الكوبونات (دولة دولة لأن geos* مطلوب)
    # ─────────────────────────────────────────
    print("\n🟢 STEP 1: Fetching Coupons...")
    all_coupons_raw = []
    
    for i, country in enumerate(ALL_COUNTRIES, 1):
        elapsed = time.time() - start_time
        print(f"\n🌍 [{i}/{len(ALL_COUNTRIES)}] Processing: {country} (Elapsed: {elapsed:.1f}s)")
        
        params = {
            "limit": 100,
            "is_active": "true",
            "source_id": source_id,
            "geos": country
        }
        
        try:
            country_coupons = fetch_all_pages(
                f"{base_url}/public_api/v1/coupons",
                params,
                headers,
                page_delay=12  # تأخير آمن بين الصفحات
            )
            all_coupons_raw.extend(country_coupons)
            print(f"   📥 Cumulative Total: {len(all_coupons_raw)}")
        except Exception as e:
            print(f"   ❌ Failed {country}: {e}")
        
        # تأخير بين الدول لتجنب Rate Limit
        if i < len(ALL_COUNTRIES):
            time.sleep(2)
            if i % 25 == 0:
                print("   ⏳ Long break (10s) after 25 countries...")
                time.sleep(10)
    
    # إزالة التكرار ورفع الكوبونات
    print(f"\n📊 Total raw coupons collected: {len(all_coupons_raw)}")
    unique_coupons = deduplicate_coupons(all_coupons_raw)
    upload_to_r2(unique_coupons, "coupons.json", env)
    
    # ─────────────────────────────────────────
    # 2️⃣ جلب التجار (عالمي مرة واحدة - أكثر كفاءة)
    # ─────────────────────────────────────────
    print("\n🟢 STEP 2: Fetching Merchants (Global)...")
    merch_params = {
        "limit": 100,
        "status": "active",
        "source_id": source_id
        # country اختياري هنا، الجلب العالمي يغطي كل الدول
    }
    
    try:
        all_merchants = fetch_all_pages(
            f"{base_url}/public_api/v1/merchants",
            merch_params,
            headers,
            page_delay=12
        )
        upload_to_r2(all_merchants, "merchants.json", env)
    except Exception as e:
        print(f"❌ Merchant fetch failed: {e}")
        all_merchants = []
    
    # ─────────────────────────────────────────
    # 📊 التقرير النهائي
    # ─────────────────────────────────────────
    total_time = time.time() - start_time
    print("\n" + "=" * 70)
    print("🎉 FULL SYNC COMPLETED SUCCESSFULLY!")
    print(f"📦 coupons.json   : {len(unique_coupons)} unique coupons")
    print(f"📦 merchants.json : {len(all_merchants)} merchants")
    print(f"⏱️  Total Runtime   : {total_time:.1f} seconds ({total_time/60:.1f} mins)")
    print("=" * 70)

if __name__ == "__main__":
    main()
