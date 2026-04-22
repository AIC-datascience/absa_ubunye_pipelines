# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Understanding Geo-coding, Flood Risk Assessments, and Network Debugging
# MAGIC ---
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook demonstrates how to:
# MAGIC - Query latitude and longitude for string-based addresses using the **TOMTOM API**  
# MAGIC - Retrieve flood-related metrics using the **JBA API**
# MAGIC - Diagnose and resolve external network connectivity issues within the ABSA network
# MAGIC
# MAGIC ## Description
# MAGIC - Demonstrates API integration for geocoding and flood risk assessment  
# MAGIC - Documents steps to resolve connectivity issues for APIs outside the ABSA network  
# MAGIC - Queries key JBA metrics:
# MAGIC   - **Floodability Score**
# MAGIC   - **Flood Depth**
# MAGIC
# MAGIC ## Author
# MAGIC **Prepared by:** [Thabang Mashinini-Sekgoto](https://absa.atlassian.net/wiki/spaces/~71202064fdb1a721f5443ab5860d8b415c065b/pages/1806828776/Flood+Risk+Model)  
# MAGIC Lead Data Scientist — AIC Data Science  
# MAGIC
# MAGIC **Created:** 2025-11-10  
# MAGIC **Last Updated:** 2025-11-10  
# MAGIC
# MAGIC ## Environment
# MAGIC - **Platform:** Databricks  
# MAGIC - **Language:** Python 3  
# MAGIC - **Libraries:** requests, pandas, json  
# MAGIC - **APIs:** TOMTOM, JBA  
# MAGIC - **Network:** Requires outbound access for external API calls.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Sections Outline
# MAGIC 1. **Imports & Environment Setup**
# MAGIC 2. **Configuration & API Keys**
# MAGIC 3. **Geocoding using TOMTOM**
# MAGIC 4. **Flood Metrics via JBA**
# MAGIC 5. **Network Debugging (ABSA Restrictions)**
# MAGIC 6. **Visualization & Analysis**
# MAGIC 7. **Summary**
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# # Run this as the first cell in your notebook to initialize Spark
# spark.sql("SELECT 1")

# # Now you can run your query
# display(
#   spark.sql(
#     "SELECT * FROM absa_sandbox_workspace.default.dev_aic_exposure_dbo_idit_asti_exposure"
#   )
# )

# COMMAND ----------

import requests
import urllib.parse
import pandas as pd
from shapely.geometry import Point
import folium
import os
from folium.plugins import MousePosition
import geopandas as gpd

# COMMAND ----------

# !pip install shapely folium geopandas

# COMMAND ----------

# # Run this as the first cell in your notebook to initialize Spark
# spark.sql("SELECT 1")

# # Now you can run your query
# display(
#   spark.sql(
#     "SELECT * FROM absa_sandbox_workspace.default.dev_aic_exposure_dbo_idit_asti_exposure"
#   )
# )

# COMMAND ----------

# MAGIC %md
# MAGIC _______
# MAGIC ## TomTom Geocoding
# MAGIC _________

# COMMAND ----------

# MAGIC %sh
# MAGIC # To test if your compute can reach the API server and connect to port 443. It’s a quick network connectivity check.
# MAGIC nc -vz api.tomtom.com 443 || echo "nc failed; try openssl s_client next"
# MAGIC #To verify the TomTom server’s SSL certificate and confirm it’s valid and trusted by your environment.
# MAGIC openssl s_client -connect api.tomtom.com:443 -showcerts </dev/null 2>/dev/null | openssl x509 -noout -issuer -subject

# COMMAND ----------

import requests
import certifi

# API endpoint
url = "https://api.tomtom.com/search/2/search/136%20Gerhard%20Street%2C%20Centurion.json?countrySet=ZA&limit=1&key=c8AMnJnJgmT8SiJIMNCai2wVNlvjnGRd"

# Custom corporate/ZScaler certificate path
custom_cert = "/Volumes/absa_sandbox_workspace/default/zscalar_certs/zscaler_fullchain.crt"

print("=== SSL Verification Test ===\n")

# 1️Test using corporate certificate
try:
    r1 = requests.get(url, verify=custom_cert, timeout=10)
    print("Custom certificate verification succeeded.")
    print(f"Status Code: {r1.status_code}")
except requests.exceptions.SSLError as e:
    print("❌ SSL verification failed with custom cert.")
    print("Error:", e)
except Exception as e:
    print("❌ Request failed (non-SSL error).")
    print("Error:", e)

print("\n---\n")

# 2️Test using default certifi CA bundle
try:
    r2 = requests.get(url, verify=certifi.where(), timeout=10)
    print("✅ Default certifi CA bundle verification succeeded.")
    print(f"Status Code: {r2.status_code}")
except requests.exceptions.SSLError as e:
    print("❌ SSL verification failed with default certifi CA bundle.")
    print("Error:", e)
except Exception as e:
    print("❌ Request failed (non-SSL error).")
    print("Error:", e)

print("\n---\n")

# 3️Test with SSL verification disabled (for debugging only)
try:
    r3 = requests.get(url, verify=False, timeout=10)
    print("⚠️ Verification disabled — request succeeded (not secure).")
    print(f"Status Code: {r3.status_code}")
except Exception as e:
    print("❌ Even insecure request failed.")
    print("Error:", e)



# COMMAND ----------

import time
import urllib.parse
import requests
import pandas as pd
import certifi
from pyspark.sql import functions as F
from pyspark.sql import Window

API_KEY = "c8AMnJnJgmT8SiJIMNCai2wVNlvjnGRd"
COUNTRY_SET = "ZA"
MIN_FUZZY = 1
MAX_FUZZY = 2
LIMIT     = 5
VIEW      = "Unified"
LANGUAGE  = "en-ZA"
PER_CALL_SLEEP = 0.15

def _tomtom_call(session, addr, params_base, timeout):
    """Try with stricter params first; if 400, fall back to simpler."""
    q = urllib.parse.quote(addr, safe="")
    url = f"https://api.tomtom.com/search/2/search/{q}.json"

    # 1) strict (with language)
    r = session.get(url, params=params_base, timeout=timeout, verify=certifi.where())
    if r.status_code == 400:
        # 2) drop language
        p2 = {k: v for k, v in params_base.items() if k != "language"}
        r = session.get(url, params=p2, timeout=timeout, verify=certifi.where())
    if r.status_code == 400:
        # 3) drop idxSet if present (some accounts don’t allow it here)
        p3 = {k: v for k, v in params_base.items() if k not in ("language", "idxSet")}
        r = session.get(url, params=p3, timeout=timeout, verify=certifi.where())
    return r

def batch_geocode(
    addresses,
    api_key=API_KEY,
    country_set=COUNTRY_SET,
    min_fuzzy=MIN_FUZZY,
    max_fuzzy=MAX_FUZZY,
    limit=LIMIT,
    view=VIEW,
    language=LANGUAGE,
    timeout=(10, 20),
    max_retries=3,
    backoff_base=0.75,
    return_top1=False
):
    s = requests.Session()
    s.headers.update({"User-Agent": "databricks-geocoder/1.0", "Tracking-ID": "absa-geo-001"})
    results_rows = []

    # base params (no idxSet by default to avoid 400s)
    base_params = {
        "countrySet": country_set,
        "minFuzzyLevel": min_fuzzy,
        "maxFuzzyLevel": max_fuzzy,
        "limit": limit,
        "view": view,
        "language": language,
        "key": api_key,
        # "idxSet": "Geo,Addr",  # <- comment out; some tenants reject this on /search
    }

    for addr in addresses:
        for attempt in range(1, max_retries + 1):
            try:
                r = _tomtom_call(s, addr, base_params, timeout)

                # handle 429
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    time.sleep(float(ra) if ra else attempt * 2.0)
                    continue

                # if still bad, capture error text for debugging
                if r.status_code >= 400:
                    err = f"{r.status_code} {r.reason}: {r.text[:300]}"
                    if attempt == max_retries:
                        results_rows.append({
                            "inputAddress": addr, "type": None, "id": None, "score": None, "scoreRank": None,
                            "freeformAddress": None, "countryCode": None, "municipality": None,
                            "streetName": None, "postalCode": None, "lat": None, "lon": None,
                            "error": err
                        })
                        break
                    else:
                        time.sleep(backoff_base * attempt)
                        continue

                payload = r.json()
                candidates = [c for c in (payload.get("results") or []) if isinstance(c, dict)]
                for rank, c in enumerate(candidates, start=1):
                    a = c.get("address") or {}
                    p = c.get("position") or {}
                    results_rows.append({
                        "inputAddress": addr,
                        "type": c.get("type"),
                        "id": c.get("id"),
                        "score": c.get("score"),
                        "scoreRank": rank,
                        "freeformAddress": a.get("freeformAddress"),
                        "countryCode": a.get("countryCode"),
                        "municipality": a.get("municipality"),
                        "streetName": a.get("streetName"),
                        "postalCode": a.get("postalCode"),
                        "lat": p.get("lat"),
                        "lon": p.get("lon"),
                        "error": None,
                    })
                break  # success

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    results_rows.append({
                        "inputAddress": addr,
                        "type": None, "id": None, "score": None, "scoreRank": None,
                        "freeformAddress": None, "countryCode": None, "municipality": None,
                        "streetName": None, "postalCode": None, "lat": None, "lon": None,
                        "error": str(e),
                    })
                else:
                    time.sleep(backoff_base * attempt)
        time.sleep(PER_CALL_SLEEP)

    if not results_rows:
        print("No geocoding results.")
        return None

    pdf = pd.DataFrame(results_rows)
    if "error" not in pdf.columns:
        pdf["error"] = None

    # sdf = spark.createDataFrame(pdf)
    # sdf = (sdf
    #        .withColumn("isTopCandidate", F.col("scoreRank") == 1)
    #        .withColumn("hasError", F.col("error").isNotNull()))

    # if return_top1:
    #     w = Window.partitionBy("inputAddress").orderBy(F.col("scoreRank").asc_nulls_last())
    #     sdf = (sdf.withColumn("rn", F.row_number().over(w))
    #              .filter(F.col("rn") == 1)
    #              .drop("rn"))
    return pdf

# Example: Calling the batch_geocode() function

addresses = [
    "136 Gerhard Street, Centurion",
    "1 Discovery Place, Sandton",
    "30 Jellicoe Avenue, Rosebank"
]

# Call the function
df_results = batch_geocode(addresses, return_top1=True)

# Display the Spark DataFrame
df_results_sub=df_results[df_results.scoreRank == 1]


# COMMAND ----------

# MAGIC %md
# MAGIC ______
# MAGIC
# MAGIC ## JBA Flood Risk Rating
# MAGIC
# MAGIC _____

# COMMAND ----------

# MAGIC %sh
# MAGIC # To test if your compute can reach the API server and connect to port 443. It’s a quick network connectivity check.
# MAGIC nc -vz api.jbarisk.com:443 || echo "nc failed; try openssl s_client next"
# MAGIC #To verify the TomTom server’s SSL certificate and confirm it’s valid and trusted by your environment.
# MAGIC openssl s_client -connect api.jbarisk.com:443 -showcerts </dev/null 2>/dev/null | openssl x509 -noout -issuer -subject

# COMMAND ----------

import requests
import certifi

# API endpoint
url = "https://api.jbarisk.com/flooddepths/ZA?geometry=POINT(28.195692 -25.842753)"

# Custom corporate/ZScaler certificate path
custom_cert = "/Volumes/absa_sandbox_workspace/default/zscalar_certs/zscaler_fullchain.crt"

print("=== SSL Verification Test ===\n")

# 1️Test using corporate certificate
try:
    r1 = requests.get(url, verify=custom_cert, timeout=10)
    print("Custom certificate verification succeeded.")
    print(f"Status Code: {r1.status_code}")
except requests.exceptions.SSLError as e:
    print("❌ SSL verification failed with custom cert.")
    print("Error:", e)
except Exception as e:
    print("❌ Request failed (non-SSL error).")
    print("Error:", e)

print("\n---\n")

# 2️Test using default certifi CA bundle
try:
    r2 = requests.get(url, verify=certifi.where(), timeout=10)
    print("✅ Default certifi CA bundle verification succeeded.")
    print(f"Status Code: {r2.status_code}")
except requests.exceptions.SSLError as e:
    print("❌ SSL verification failed with default certifi CA bundle.")
    print("Error:", e)
except Exception as e:
    print("❌ Request failed (non-SSL error).")
    print("Error:", e)

print("\n---\n")

# 3️Test with SSL verification disabled (for debugging only)
try:
    r3 = requests.get(url, verify=False, timeout=10)
    print("⚠️ Verification disabled — request succeeded (not secure).")
    print(f"Status Code: {r3.status_code}")
except Exception as e:
    print("❌ Even insecure request failed.")
    print("Error:", e)



# COMMAND ----------

# import json
# import requests

# # Usage example
# username = 'Liam.Culligan'
# password = 'WFks46=-e%Ez3#=!'
# url = "https://api.jbarisk.com/flooddepths/ZA?geometry=POINT(28.195692 -25.842753)"

# with requests.Session() as session:
#     # Set up basic authentication
#     session.auth = (username, password)
#     response = session.get(url)
#     if response.status_code == 200:
#         # Request was successful
#         print(response.json())
#     else:
#         # Request failed
#         print(f'Request failed with status code: {response.status_code}')

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt as shapely_wkt

def ensure_wgs84_points(df, lon_col="lon", lat_col="lat", id_col="id"):
    """Create a WGS84 GeoDataFrame with Point(lon, lat)."""
    df = df.copy()
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(xy) for xy in zip(df[lon_col], df[lat_col])],
        crs="EPSG:4326"
    )
    # ensure id exists and is stringy for merging
    if id_col not in gdf.columns:
        gdf[id_col] = gdf.index.astype(str)
    else:
        gdf[id_col] = gdf[id_col].astype(str)
    return gdf

def gdf_points_to_jba_items(gdf, id_col="id", buffer_m=100):
    """Convert point GeoDataFrame to JBA request 'geometries' list with WKT and buffer."""
    if buffer_m > 500:
        raise ValueError("JBA limit: buffer must be ≤ 500 meters for points.")
    if gdf.crs is None or gdf.crs.to_string() != "EPSG:4326":
        raise ValueError("Input must be EPSG:4326 (WGS84). Reproject before calling.")
    items = []
    for _, row in gdf.iterrows():
        pt = row.geometry
        if not isinstance(pt, Point):
            continue
        wkt_geom = pt.wkt  # POINT(<lon> <lat>)
        items.append({
            "id": str(row[id_col]),
            "wkt_geometry": wkt_geom,
            "buffer": int(buffer_m)
        })
    return items

import time
import math
import requests
from pandas import json_normalize


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def jba_batch_request(
    endpoint_url,           # e.g. "https://api.jbarisk.com/floodscores/ZA"
    items,                  # list of {"id","wkt_geometry","buffer"}
    basic_auth_token,       # e.g. "Basic <base64-cred>"
    country_code="ZA",
    include_version=True,
    batch_size=10,
    max_batches=100,
    timeout=(20, 60),
    max_retries=3,
    backoff_base=1.5,
    verify_tls=True,        # or path to corp CA bundle (e.g., "/dbfs/.../zscaler_fullchain.crt")
):
    """
    Sends batched POSTs to JBA endpoint and returns a list of response items.
    """
    if len(items) > batch_size * max_batches:
        raise ValueError(f"Too many items: JBA limit ~{batch_size} per batch x {max_batches} batches.")

    headers = {
        "Authorization": basic_auth_token,
        "Content-Type": "application/json"
    }
    params = {"include_version": include_version}
    all_results = []

    num_batches = math.ceil(len(items) / batch_size) if items else 0
    for b_idx, batch in enumerate(_chunks(items, batch_size), start=1):
        body = {
            "country_code": country_code,
            "geometries": batch
        }
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.post(endpoint_url, headers=headers, params=params,
                                  json=body, timeout=timeout, verify=verify_tls)

                # handle rate limit
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if ra else attempt * 2.0
                    time.sleep(sleep_s)
                    continue

                if r.status_code >= 400:
                    # capture error, raise on last attempt
                    msg = f"[Batch {b_idx}/{num_batches}] {r.status_code} {r.reason}: {r.text[:500]}"
                    if attempt == max_retries:
                        raise RuntimeError(msg)
                    time.sleep(backoff_base * attempt)
                    continue

                payload = r.json() or {}
                # The exact structure depends on JBA; most responses echo an array keyed by 'results'/'data'
                # Here we try common keys; adjust if your payload differs.

                all_results.extend(payload)
                # break  # success for this batch

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise
                time.sleep(backoff_base * attempt)

        # polite pacing between batches
        time.sleep(0.2)
    # print(all_results)
    all_results= json_normalize(all_results,sep='.')

    return all_results


# COMMAND ----------

df_results_sub

# COMMAND ----------

df_results_sub

# COMMAND ----------

# 0) Prep inputs
BASIC_AUTH = "Base TGlhbS5DdWxsaWdhbjpXRmtzNDY9LWUlRXozIz0h"  # do NOT hardcode secrets in prod; use a secret scope
COUNTRY    = "ZA"
BUFFER_M   = 100  # 100m default; must be <= 500

gdf_pts = ensure_wgs84_points(df_results_sub, lon_col="lon", lat_col="lat", id_col="id")
items    = gdf_points_to_jba_items(gdf_pts, id_col="id", buffer_m=BUFFER_M)

# 1) Call floodscores
scores_endpoint = "https://api.jbarisk.com/floodscores/ZA"
df_scores = jba_batch_request(
    endpoint_url=scores_endpoint,
    items=items,
    basic_auth_token=BASIC_AUTH,
    country_code=COUNTRY,
    include_version=True,
    batch_size=10,
    max_batches=100,
    timeout=(30, 120),
    verify_tls=True  # or a path to your corporate bundle if needed
)

# 2) Call flooddepths
depths_endpoint = "https://api.jbarisk.com/flooddepths/ZA"
df_depths = jba_batch_request(
    endpoint_url=depths_endpoint,
    items=items,
    basic_auth_token=BASIC_AUTH,
    country_code=COUNTRY,
    include_version=True,
    batch_size=10,
    max_batches=100,
    timeout=(30, 120),
    verify_tls=True
)


# # 4) Merge both sets back to your GeoDataFrame by 'id'
gdf_out_final = df_depths.merge(df_scores, on="id", how="inner")
# gdf_out = gdf_out.merge(df_depths, on="id", how="left", suffixes=("", "_depth"))

# gdf_out.crs = "EPSG:4326"  # make explicit for downstream users
# gdf_out.head()


# COMMAND ----------

df_results_sub

# COMMAND ----------

gdf_out_final = gdf_out_final.drop_duplicates(subset=['id'], keep='first')
gdf_out_final = df_results_sub.merge(gdf_out_final, on="id", how="inner" )


# COMMAND ----------

# import requests

# import json


# # API URL

# url = "https://api.jbarisk.com/flooddepths/ZA"

 

# # Query parameters

# params = {

#     "include_version": True,

# }

 

# # Headers, fill with existing headers

# headers = {"Authorization": "Base TGlhbS5DdWxsaWdhbjpXRmtzNDY9LWUlRXozIz0h"}

 

# # Request body

# body = {

#     "country_code": "ZA",

#     "geometries": [

#         {"id": "id1", "wkt_geometry": "POINT(28.195692 -25.842753)", "buffer": 10},

#         {"id": "id2", "wkt_geometry": "POINT(28.195692 -25.842753)", "buffer": 10},

#     ]

# }

 

# response = requests.post(url, headers=headers, params=params, data=json.dumps(body))

# if response.status_code == 200:
#     print(response.json())
# else:
#     print(f"Request failed with status code: {response.status_code}")
#     print(response.text)

 

# COMMAND ----------

# Updated mapping dictionary with spaces replaced by underscores, lower case, and "m" replaced with "meters" and "%" with "percentage"
column_mapping = {
    'id': 'AddressKey',
    'stats.FLRF_U.rp_20.ppa20': 'river_flood_rp20_affected_area_percentage',
    'stats.FLRF_U.rp_20.min20': 'river_flood_rp20_min_depth_meters',
    'stats.FLRF_U.rp_20.max20': 'river_flood_rp20_max_depth_meters',
    'stats.FLRF_U.rp_20.mean': 'river_flood_rp20_mean_depth_meters',
    'stats.FLRF_U.rp_20.std_dev': 'river_flood_rp20_std_dev_depth_meters',
    'stats.FLRF_U.rp_50.ppa50': 'river_flood_rp50_affected_area_percentage',
    'stats.FLRF_U.rp_50.min50': 'river_flood_rp50_min_depth_meters',
    'stats.FLRF_U.rp_50.max50': 'river_flood_rp50_max_depth_meters',
    'stats.FLRF_U.rp_50.mean': 'river_flood_rp50_mean_depth_meters',
    'stats.FLRF_U.rp_50.std_dev': 'river_flood_rp50_std_dev_depth_meters',
    'stats.FLRF_U.rp_100.ppa100': 'river_flood_rp100_affected_area_percentage',
    'stats.FLRF_U.rp_100.min100': 'river_flood_rp100_min_depth_meters',
    'stats.FLRF_U.rp_100.max100': 'river_flood_rp100_max_depth_meters',
    'stats.FLRF_U.rp_100.mean': 'river_flood_rp100_mean_depth_meters',
    'stats.FLRF_U.rp_100.std_dev': 'river_flood_rp100_std_dev_depth_meters',
    'stats.FLRF_U.rp_200.ppa200': 'river_flood_rp200_affected_area_percentage',
    'stats.FLRF_U.rp_200.min200': 'river_flood_rp200_min_depth_meters',
    'stats.FLRF_U.rp_200.max200': 'river_flood_rp200_max_depth_meters',
    'stats.FLRF_U.rp_200.mean': 'river_flood_rp200_mean_depth_meters',
    'stats.FLRF_U.rp_200.std_dev': 'river_flood_rp200_std_dev_depth_meters',
    'stats.FLRF_U.rp_500.ppa500': 'river_flood_rp500_affected_area_percentage',
    'stats.FLRF_U.rp_500.min500': 'river_flood_rp500_min_depth_meters',
    'stats.FLRF_U.rp_500.max500': 'river_flood_rp500_max_depth_meters',
    'stats.FLRF_U.rp_500.mean': 'river_flood_rp500_mean_depth_meters',
    'stats.FLRF_U.rp_500.std_dev': 'river_flood_rp500_std_dev_depth_meters',
    'stats.FLRF_U.rp_1500.ppa1500': 'river_flood_rp1500_affected_area_percentage',
    'stats.FLRF_U.rp_1500.min1500': 'river_flood_rp1500_min_depth_meters',
    'stats.FLRF_U.rp_1500.max1500': 'river_flood_rp1500_max_depth_meters',
    'stats.FLRF_U.rp_1500.mean': 'river_flood_rp1500_mean_depth_meters',
    'stats.FLRF_U.rp_1500.std_dev': 'river_flood_rp1500_std_dev_depth_meters',
    'stats.FLRF_U.sop': 'river_flood_standard_of_protection_rp',
    'stats.FLSW_U.rp_20.ppa20': 'surface_water_flood_rp20_affected_area_percentage',
    'stats.FLSW_U.rp_20.min20': 'surface_water_flood_rp20_min_depth_meters',
    'stats.FLSW_U.rp_20.max20': 'surface_water_flood_rp20_max_depth_meters',
    'stats.FLSW_U.rp_20.mean': 'surface_water_flood_rp20_mean_depth_meters',
    'stats.FLSW_U.rp_20.std_dev': 'surface_water_flood_rp20_std_dev_depth_meters',
    'stats.FLSW_U.rp_50.ppa50': 'surface_water_flood_rp50_affected_area_percentage',
    'stats.FLSW_U.rp_50.min50': 'surface_water_flood_rp50_min_depth_meters',
    'stats.FLSW_U.rp_50.max50': 'surface_water_flood_rp50_max_depth_meters',
    'stats.FLSW_U.rp_50.mean': 'surface_water_flood_rp50_mean_depth_meters',
    'stats.FLSW_U.rp_50.std_dev': 'surface_water_flood_rp50_std_dev_depth_meters',
    'stats.FLSW_U.rp_100.ppa100': 'surface_water_flood_rp100_affected_area_percentage',
    'stats.FLSW_U.rp_100.min100': 'surface_water_flood_rp100_min_depth_meters',
    'stats.FLSW_U.rp_100.max100': 'surface_water_flood_rp100_max_depth_meters',
    'stats.FLSW_U.rp_100.mean': 'surface_water_flood_rp100_mean_depth_meters',
    'stats.FLSW_U.rp_100.std_dev': 'surface_water_flood_rp100_std_dev_depth_meters',
    'stats.FLSW_U.rp_200.ppa200': 'surface_water_flood_rp200_affected_area_percentage',
    'stats.FLSW_U.rp_200.min200': 'surface_water_flood_rp200_min_depth_meters',
    'stats.FLSW_U.rp_200.max200': 'surface_water_flood_rp200_max_depth_meters',
    'stats.FLSW_U.rp_200.mean': 'surface_water_flood_rp200_mean_depth_meters',
    'stats.FLSW_U.rp_200.std_dev': 'surface_water_flood_rp200_std_dev_depth_meters',
    'stats.FLSW_U.rp_500.ppa500': 'surface_water_flood_rp500_affected_area_percentage',
    'stats.FLSW_U.rp_500.min500': 'surface_water_flood_rp500_min_depth_meters',
    'stats.FLSW_U.rp_500.max500': 'surface_water_flood_rp500_max_depth_meters',
    'stats.FLSW_U.rp_500.mean': 'surface_water_flood_rp500_mean_depth_meters',
    'stats.FLSW_U.rp_500.std_dev': 'surface_water_flood_rp500_std_dev_depth_meters',
    'stats.FLSW_U.rp_1500.ppa1500': 'surface_water_flood_rp1500_affected_area_percentage',
    'stats.FLSW_U.rp_1500.min1500': 'surface_water_flood_rp1500_min_depth_meters',
    'stats.FLSW_U.rp_1500.max1500': 'surface_water_flood_rp1500_max_depth_meters',
    'stats.FLSW_U.rp_1500.mean': 'surface_water_flood_rp1500_mean_depth_meters',
    'stats.FLSW_U.rp_1500.std_dev': 'surface_water_flood_rp1500_std_dev_depth_meters',
    'River_Floodscore_Def': 'river_flood_score_defended',
    'River_Floodscore_UD': 'river_flood_score_undefended',
    'Surfacewater_Floodscore_UD': 'surface_water_flood_score_undefended',
    'FloodScore_Def': 'overall_flood_score_defended',
    'FloodScore_UD': 'overall_flood_score_undefended',
    'Floodability_Def': 'floodability_defended',
    'Floodability_UD': 'floodability_undefended'
}

# Rename the columns using the mapping dictionary
gdf_out_final.rename(columns=column_mapping, inplace=True)
print("Final DataFrame with Renamed Columns:")
# columns = [ 'wkt_geometry', 'buffer']
# gdf_out_final  = gdf_out_final.drop(columns, axis=1)
gdf_out_final.columns

# COMMAND ----------

gdf_out_final

# COMMAND ----------


