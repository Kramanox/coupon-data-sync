#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🌍 Coupon Data Full Sync Script - 6 Parallel Jobs + Per-Country Files
✅ كل دولة لها ملف منفصل: coupons_DZ.json, coupons_FR.json ...
✅ لا فلترة - البيانات كاملة كما هي من API
✅ 6 Jobs متوازية لأقصى سرعة
✅ التجار في ملف واحد: merchants.json
"""

import os
import time
import json
import requests
import boto3
from botocore.config import Config

# ─────────────────────────────────────────────
# 🌍 قائمة جميع الدول (250 دولة)
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
# 🔧 التحقق من صحة R2_ENDPOINT_URL
# ─────────────────────────────────────────────
def validate_r2_endpoint(url):
    """يتأكد أن URL صالح ويبدأ بـ https://"""
    if not url:
        raise ValueError("❌ R2_ENDPOINT_URL is empty! Please set it in GitHub Secrets.")
    url = url.strip()
    if not url.startswith("https://") and not url.startswith("http://"):
        raise ValueError(
            f"❌ R2_ENDPOINT_URL must start with https:// — Got: '{url}'\n"
            "  Example: https://ACCOUNT_ID.r2.cloudflarestorage.com"
        )
    return url

# ─────────────────────────────────────────────
# 📥 جلب الصفحات مع Pagination الكاملة
# ─────────────────────────────────────────────
def fetch_all_pages(base_endpoint, params, headers, page_delay=8, max_retries=3):
    """يجلب جميع الصفحات مع إعادة المحاولة عند الفشل"""
    results = []
    offset = 0
    page_count = 0

    while True:
        params["offset"] = offset
        print(f"   📄 Page {page_count + 1} | Offset: {offset}")

        success = False
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    base_endpoint, params=params, headers=headers, timeout=45
                )

                if response.status_code == 400:
                    print(f"   ⚠️  Bad Request (400): {response.text[:200]}")
                    return results

                if response.status_code == 429:
                    wait = 30 * attempt
                    print(f"   ⚠️  Rate limited (429) — waiting {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()
                page_results = data.get("results", [])
                results.extend(page_results)

                print(f"   ✅ Got {len(page_results)} items | Total: {len(results)}")

                if not data.get("next") or len(page_results) == 0:
                    print("   🏁 End of pagination.")
                    return results

                offset += len(page_results)
                page_count += 1

                if page_count > 0:
                    print(f"   ⏳ Waiting {page_delay}s...")
                    time.sleep(page_delay)

                success = True
                break

            except requests.exceptions.RequestException as e:
                print(f"   ⚠️  Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(15 * attempt)
                else:
                    if page_count == 0:
                        raise
                    print(f"   ❌ Giving up after {max_retries} retries.")
                    return results

        if not success:
            return results

    return results

# ─────────────────────────────────────────────
# ☁️ رفع ملف JSON واحد إلى R2
# ─────────────────────────────────────────────
def upload_to_r2(data, filename, s3_client, bucket_name):
    """يرفع قائمة أو dict إلى R2 كـ JSON"""
    print(f"☁️  Uploading {filename} ({len(data) if isinstance(data, list) else '?'} items)...")
    json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    s3_client.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    print(f"   ✅ Uploaded {filename}")

# ─────────────────────────────────────────────
# 🏪 جلب التجار (مرة واحدة لكل Job)
# ─────────────────────────────────────────────
def fetch_merchants(base_url, source_id, headers, page_delay=8):
    print("\n🟢 Fetching Merchants...")
    params = {
        "limit": 100,
        "status": "active",
        "source_id": source_id
    }
    try:
        merchants = fetch_all_pages(
            f"{base_url}/public_api/v1/merchants",
            params,
            headers,
            page_delay=page_delay
        )
        print(f"   📦 Total merchants: {len(merchants)}")
        return merchants
    except Exception as e:
        print(f"   ❌ Merchant fetch failed: {e}")
        return []

# ─────────────────────────────────────────────
# 🚀 الدالة الرئيسية
# ─────────────────────────────────────────────
def main():
    # ── قراءة متغيرات البيئة ──────────────────
    base_url   = os.environ["API_BASE_URL"].rstrip('/')
    api_token  = os.environ["API_TOKEN"]
    source_id  = os.environ.get("SOURCE_ID", "")
    bucket     = os.environ["R2_BUCKET_NAME"]

    # ✅ التحقق من صحة R2_ENDPOINT_URL قبل أي شيء
    r2_endpoint = validate_r2_endpoint(os.environ.get("R2_ENDPOINT_URL", ""))

    # ── نطاق الدول لهذا الـ Job ───────────────
    country_range    = os.environ.get("COUNTRY_RANGE", "0-249")
    start_idx, end_idx = map(int, country_range.split('-'))
    countries        = ALL_COUNTRIES[start_idx:end_idx + 1]

    # ── هل هذا الـ Job هو المسؤول عن التجار؟ ──
    # فقط الـ Job الأول (0-41) يجلب التجار لتجنب التكرار
    is_merchant_job = (start_idx == 0)

    job_id = os.environ.get("GITHUB_JOB", f"part-{start_idx}")

    headers = {
        "Authorization": api_token,
        "Accept": "application/json"
    }


# ── إنشاء عميل S3/R2 ──────────────────────
    s3 = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"].strip(),
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"].strip(),
        region_name="auto",  # 👈 هذا السطر ضروري جداً لضبط توقيع Cloudflare R2
        config=Config(signature_version="s3v4")
    )


    print("=" * 70)
    print(f"🌍 SYNC JOB [{job_id}] — Countries [{start_idx}–{end_idx}]")
    print(f"📡 API Base   : {base_url}")
    print(f"☁️  R2 Bucket : {bucket}")
    print(f"📊 Countries  : {len(countries)}")
    print(f"🏪 Fetch merchants: {'YES' if is_merchant_job else 'NO (handled by Job 1)'}")
    print("=" * 70)

    start_time   = time.time()
    total_uploaded = 0

    # ─────────────────────────────────────────
    # 1️⃣ كوبونات — ملف منفصل لكل دولة
    # ─────────────────────────────────────────
    print("\n🟢 STEP 1: Fetching & Uploading Coupons per Country...\n")

    for i, country in enumerate(countries, 1):
        elapsed = time.time() - start_time
        print(f"\n🌍 [{i}/{len(countries)}] {country}  (Elapsed: {elapsed:.0f}s)")

        params = {
            "limit":     100,
            "is_active": "true",
            "source_id": source_id,
            "geos":      country
        }

        try:
            coupons = fetch_all_pages(
                f"{base_url}/public_api/v1/coupons",
                params,
                headers,
                page_delay=6
            )

            # ── رفع فوري لكل دولة — لا حاجة للانتظار حتى النهاية
            filename = f"coupons_{country}.json"
            upload_to_r2(coupons, filename, s3, bucket)
            total_uploaded += 1
            print(f"   📦 {country}: {len(coupons)} coupons → {filename}")

        except Exception as e:
            print(f"   ❌ Failed {country}: {e}")
            # ارفع ملف فارغ لتجنب الخطأ في البوت
            try:
                upload_to_r2([], f"coupons_{country}.json", s3, bucket)
            except Exception:
                pass

        # تأخير بسيط بين الدول (ليس بين الصفحات)
        if i < len(countries):
            time.sleep(1)

    # ─────────────────────────────────────────
    # 2️⃣ التجار — يُجلب فقط من الـ Job الأول
    # ─────────────────────────────────────────
    if is_merchant_job:
        print("\n🟢 STEP 2: Fetching Merchants (Global — Job 1 only)...")
        merchants = fetch_merchants(base_url, source_id, headers, page_delay=6)
        if merchants:
            upload_to_r2(merchants, "merchants.json", s3, bucket)
        else:
            print("   ⚠️  No merchants fetched — skipping upload.")
    else:
        print(f"\n⏭️  STEP 2: Skipping merchants (handled by Job 1).")

    # ─────────────────────────────────────────
    # 📊 التقرير النهائي
    # ─────────────────────────────────────────
    total_time = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"🎉 JOB DONE — [{job_id}]  Range [{start_idx}–{end_idx}]")
    print(f"📦 Countries uploaded : {total_uploaded} / {len(countries)}")
    print(f"⏱️  Runtime            : {total_time:.0f}s  ({total_time / 60:.1f} min)")
    print("=" * 70)


if __name__ == "__main__":
    main()
