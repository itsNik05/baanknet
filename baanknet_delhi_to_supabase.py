import requests
import time
import urllib3
import os
from datetime import datetime
from supabase import create_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -----------------------------
# SUPABASE CONFIG
# -----------------------------
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing SUPABASE_URL or SUPABASE_KEY environment variables")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "baanknet_properties"

# -----------------------------
# BAANKNET CONFIG
# -----------------------------
LIST_API = "https://baanknet.com/eauction-psb/api/property-listing-data/1"

COOKIES = {
    "JSESSIONID": "6d0a03c3-4e72-417d-a516-934db482fa6e",
    "language": "en",
    "languageId": "1",
    "XSRF-TOKEN": "6322de5c-c7d4-4b08-8225-c9838c1dc24a"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": "https://baanknet.com/property-listing",
    "Origin": "https://baanknet.com"
}

PAYLOAD = {
    "state": "Delhi",
    "typeOfAction": [],
    "stateId": "9",
    "searchType": "",
    "priceFrom": "0",
    "priceTo": "50000000000",
    "sortBy": "3"
}


# -----------------------------
# HELPERS
# -----------------------------
def clean_text(text):
    if not text:
        return None
    return str(text).replace("\r", " ").replace("\n", " ").strip()


def parse_datetime(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%d-%m-%Y %H:%M:%S").isoformat()
    except:
        return None


def fetch_listing_page(page, size=50):
    url = f"{LIST_API}?page={page}&size={size}"

    r = requests.post(url, headers=HEADERS, cookies=COOKIES, json=PAYLOAD, verify=False)

    if r.status_code != 200:
        print("❌ Listing failed:", r.status_code)
        return []

    js = r.json()
    return js.get("data", [])


def save_to_supabase(item):
    property_id = item.get("propertyId")
    if not property_id:
        return False

    record = {
        "property_id": str(property_id),

        "possession_type": item.get("possessionType"),
        "photos": item.get("photos"),
        "ownership_type": item.get("owenershipType"),
        "bank_name": item.get("bankName"),
        "summary_desc": clean_text(item.get("summaryDesc")),

        "city": item.get("city"),
        "localities": clean_text(item.get("localities")),
        "carpet_area": item.get("carpetArea"),
        "builtup_area": item.get("builtupArea"),

        "furnished_status": item.get("furnishedStatus"),
        "floor_no": str(item.get("floorNo")) if item.get("floorNo") is not None else None,
        "total_no_of_floors": str(item.get("totalNoOfFloors")) if item.get("totalNoOfFloors") is not None else None,
        "facing": item.get("facing"),

        "is_auction_created": item.get("isAuctioncreated"),
        "coordinate": clean_text(item.get("coordinate")),
        "pincode": item.get("pincode"),
        "bank_property_id": item.get("bankPropertyId"),

        "unit_of_measure_id": item.get("unitOfMeasureId"),
        "unit_of_measure": item.get("unitOfMeasure"),

        "address": clean_text(item.get("address")),
        "state_name": item.get("statename"),
        "district_name": item.get("districtname"),

        "inspection_start": parse_datetime(item.get("inspectionStartDateTime")),
        "inspection_end": parse_datetime(item.get("inspectionEndDateTime")),
        "auction_start": parse_datetime(item.get("auctionStartDateTime")),
        "auction_end": parse_datetime(item.get("auctionEndDateTime")),
        "emd_start": parse_datetime(item.get("emdStartDateTime")),
        "emd_end": parse_datetime(item.get("emdEndDateTime")),

        "raw_data": item,
        "updated_at": datetime.utcnow().isoformat()
    }

    supabase.table(TABLE_NAME).upsert(record).execute()
    return True


# -----------------------------
# MAIN
# -----------------------------
def main():
    page = 0
    size = 50
    total_saved = 0

    while True:
        print(f"\n📄 Fetching page {page}...")
        rows = fetch_listing_page(page, size=size)

        if not rows:
            print("✅ No more properties found.")
            break

        for item in rows:
            ok = save_to_supabase(item)
            if ok:
                total_saved += 1
                print(f"✅ Saved property_id={item.get('propertyId')} | Total: {total_saved}")

            time.sleep(0.2)

        page += 1
        time.sleep(0.5)

    print("\n🎉 Completed!")
    print("Total records saved:", total_saved)


if __name__ == "__main__":
    main()
