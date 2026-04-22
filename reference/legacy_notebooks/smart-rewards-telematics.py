

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, min as spark_min
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset, MonthEnd
from pytz import timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, greatest, date_format, abs, when, last_day, add_months, to_date, mean as _mean, stddev as _stddev, when
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow.spark
from pyspark.sql.functions import col, mean as _mean, stddev as _stddev, when
from scipy.stats import norm
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, min as spark_min
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset, MonthEnd
from pytz import timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, greatest, date_format, abs, when, last_day, add_months, to_date, mean as _mean, stddev as _stddev, when
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow.spark
from pyspark.sql.functions import col, mean as _mean, stddev as _stddev, when
from scipy.stats import norm
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank
from decimal import Decimal
%md
###### Load data
# 2. Load Data
transports = spark.sql("""SELECT * FROM prod_activate_telematics_transports""")
# exposure_table=  spark.sql("""SELECT * FROM prod_telematics_policy_device_mapping_exposure""")
exposure_table = spark.sql("""SELECT * FROM dev_telematics_policy_device_mapping_exposure""")
user_imei_table = spark.sql("""SELECT * FROM prod_activate_telematics_user_imei""")
cycle_rewards= spark.sql("""SELECT * FROM prod_activate_telematics_cycle_rewards""")
idit_prod_table = spark.sql("""SELECT * FROM prod_idit_rewards""")
monthly_rewards = spark.sql("""SELECT * FROM prod_monthly_rewards""")
%md

# transports.columns
from pyspark.sql import functions as F

# Ensure end_ts is treated as timestamp
transports_with_ts = transports.withColumn(
    "end_ts_cast", F.to_timestamp("end_ts")
)

# Get the row with the latest end_ts
latest_record = (
    transports_with_ts
    .orderBy(F.col("end_ts_cast").desc())
    .limit(1)
)

display(latest_record)

%md
###### The month we are calculating for

import calendar
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

# Input effective year-month as a string
input_effective_year_month = "202510"  # <-- set per run

# Parse year and month
year_1 = int(input_effective_year_month[:4])
month_1 = int(input_effective_year_month[4:])

# Python datetimes for month starts
start_current_month_date = pd.to_datetime(datetime(year_1, month_1, 1))
start_prev_month_date    = start_current_month_date - relativedelta(months=1)
start_next_month_date    = start_current_month_date + relativedelta(months=1)

# Spark DATE literals (always use DATEs for boundary comparisons)
start_current_date = F.to_date(F.lit(start_current_month_date.strftime("%Y-%m-%d")))
start_prev_date    = F.to_date(F.lit(start_prev_month_date.strftime("%Y-%m-%d")))
start_next_date    = F.to_date(F.lit(start_next_month_date.strftime("%Y-%m-%d")))

# Optional: confirm
print("Start of current month (py):", start_current_month_date)
print("Start of previous month (py):", start_prev_month_date)

# Last day of current month (string, if you still need it elsewhere)
last_day = calendar.monthrange(year_1, month_1)[1]
reward_calculation_date = f"{year_1}-{month_1:02d}-{last_day:02d}"
print("reward_calculation_date:", reward_calculation_date)

%md
##### Joining exposure table to find assets with devices
from pyspark.sql import functions as F, Window


# Step 1: Filter exposure_table by provided effective_year_month
filtered_exposure = exposure_table.filter(
    F.col("effective_year_month") >= input_effective_year_month
)


# define the window partitioned by policy_number and item_no
window_spec = Window.partitionBy("policy_number", "item_no").orderBy(F.col("inserted_timestamp").desc())

# add a row number per group ordered by latest timestamp
filtered_exposure_with_rank = filtered_exposure.withColumn("row_num", F.row_number().over(window_spec))

# keep only the latest record for each policy_number + item_no
filtered_exposure_latest = filtered_exposure_with_rank.filter(F.col("row_num") == 1).drop("row_num")

display(filtered_exposure_latest.filter(col("policy_number") == "6552986273").filter(col("imei_number") == "868373072277744"))
# Step 2: Join with user_imei_table (one-to-one match on imei_number)
exposure_joined = filtered_exposure_latest.join(
    user_imei_table,
    on="imei_number",
    how="inner"
)

# Optional: Preview result
exposure_joined.select(
    "policy_number", "item_no", "imei_number", "sentiance_id"
).show(10)


display (exposure_joined.filter(col("policy_number") == "6555598922").filter(col("imei_number") == "868373074405053"))
print(f"Number of assets before removing cancelled assets : {exposure_joined.count()}")    
%md
##### Selecting right cancelation date
from pyspark.sql import functions as F, Window

# ---------- 0) Base input ----------
src = exposure_joined

# If effective_year_month is a string like "YYYYMM", cast to int for sorting.
# (If it's already numeric, this cast is harmless.)
src = src.withColumn("effective_year_month_int", F.col("effective_year_month").cast("int"))

# ---------- 1) Choose ONE row per (imei_number, policy_number, item_no)
# Using ONLY effective_year_month for ordering (earliest month first).
keys = ["imei_number", "policy_number", "item_no"]

w = Window.partitionBy(*keys).orderBy(F.col("effective_year_month_int").asc())

ranked = src.withColumn("rn", F.row_number().over(w))
first_rows = ranked.filter(F.col("rn") == 1).drop("rn")

# ---------- 2) Decide the item_cancellation_date per group
# If all null -> NULL, else take earliest non-null date
agg_dates = (
    src.groupBy(*keys)
       .agg(
           F.sum(F.when(F.col("item_cancellation_date").isNotNull(), 1).otherwise(0)).alias("non_null_cnt"),
           F.min("item_cancellation_date").alias("min_item_cxl")
       )
       .withColumn(
           "item_cancellation_date_chosen",
           F.when(F.col("non_null_cnt") == 0, F.lit(None).cast("date")).otherwise(F.col("min_item_cxl"))
       )
       .select(*keys, "item_cancellation_date_chosen")
)

# ---------- 3) Attach the chosen cancellation date to the picked row ----------
dedup = (
    first_rows.alias("f")
    .join(agg_dates.alias("a"), on=keys, how="left")
    .drop("item_cancellation_date")  # replace original with the chosen value
    .withColumn("item_cancellation_date", F.col("item_cancellation_date_chosen"))
    .drop("item_cancellation_date_chosen", "effective_year_month_int")
)

# ---------- 4) (Optional) sanity check: exactly one row per key ----------
dupes = dedup.groupBy(*keys).count().filter("count > 1").limit(1).count()
if dupes > 0:
    raise RuntimeError("Deduplication failed: found duplicate (imei_number, policy_number, item_no) in output.")

# ---------- 5) Save to Delta table ----------
target_table = "prod_activate_telematics_exposure_dedup"
# dedup.write.format("delta").mode("overwrite").saveAsTable(target_table)
exposure_joined_cancelation_date_enhanced = dedup
print(f"✅ Wrote {dedup.count()} rows to {target_table}")

# display (exposure_joined_cancelation_date_enhanced.filter(F.col("policy_number") == "6543178368"))
%md
#### Removing all canceled items
# from pyspark.sql.functions import col, lit, to_date, add_months, last_day

# # Step 1: Convert manual date to a column
# exposure_joined = exposure_joined.withColumn("item_cancellation_date_cast", to_date("item_cancellation_date"))

# exposure_joined = exposure_joined.withColumn(
#     "end_ts_date", to_date(lit(reward_calculation_date))
# )

# # Step 2: Calculate maturation date (3 months after end_ts_date, to end of that month)
# exposure_joined = exposure_joined.withColumn(
#     "maturation_date",
#     last_day(add_months(col("end_ts_date"), 3))
# )

# # Step 3: Keep records where:
# # - No cancellation date (still active), OR
# # - Maturation date is on or before the cancellation date
# exposure_joined = exposure_joined.filter(
#     col("item_cancellation_date_cast").isNull() |
#     (col("maturation_date") <= col("item_cancellation_date_cast"))
# )
###########################################################################################################################################################################################
from pyspark.sql.functions import col, lit, to_date, add_months, last_day, trim

# --- 0) If reward_calculation_date is a 'YYYY-MM-DD' string, keep as is ---
# reward_calculation_date = "2025-07-31"  # example

# --- 1) Cast cancellation date and create end_ts_date (from reward_calculation_date) ---
exposure_joined_cancelation_date_enhanced = exposure_joined_cancelation_date_enhanced.withColumn(
    "item_cancellation_date_cast", to_date(col("item_cancellation_date"))
).withColumn(
    "end_ts_date", to_date(lit(reward_calculation_date))
)

# --- 2) Maturation date: 3 months after end_ts_date, snapped to month end ---
exposure_joined_cancelation_date_enhanced = exposure_joined_cancelation_date_enhanced.withColumn(
    "maturation_date",
    last_day(add_months(col("end_ts_date"), 3))
)

# --- 3) Normalize statuses (optional but safer if there are stray spaces) ---
exposure_joined_cancelation_date_enhanced = exposure_joined_cancelation_date_enhanced \
                                 .withColumn("policy_status_norm", trim(col("policy_status"))) \
                                 .withColumn("cover_status_norm",  trim(col("cover_status")))

# --- 4) Apply enhanced filter:
# Keep only where BOTH statuses are 'Policy' AND (no cancel OR maturation_date <= cancel_date)
exposure_joined_cancelation_date_enhanced = exposure_joined_cancelation_date_enhanced.filter(
    (col("policy_status_norm") == "Policy") &
    (col("cover_status_norm")  == "Policy") &
    (
     col("item_cancellation_date_cast").isNull() |
     (col("policy_status_norm") == "Policy") &
     (col("cover_status_norm")  == "Policy") &
        (col("maturation_date") <= col("item_cancellation_date_cast"))
    )
)

# (Optional) Drop the normalized helper columns if you don't need them downstream
exposure_remove_canceled = exposure_joined_cancelation_date_enhanced.drop("policy_status_norm", "cover_status_norm")

# display (exposure_remove_canceled.filter(F.col("policy_number") == "6543178368"))
print(exposure_remove_canceled.filter(F.col("premium_collected") <= "0").count())
print(f"Number of assets the rewards evaluated for : {exposure_remove_canceled.count()}")    
%md
#### Total number of  policies, imei numbers and item numbers 


from pyspark.sql.functions import countDistinct

# Count unique policy_number
unique_policies = exposure_remove_canceled.select(countDistinct("policy_number").alias("unique_policies")).collect()[0]["unique_policies"]

# Count unique imei_number
unique_imeis = exposure_remove_canceled.select(countDistinct("imei_number").alias("unique_imeis")).collect()[0]["unique_imeis"]

# Count unique item_no
unique_items = exposure_remove_canceled.select(countDistinct("item_no").alias("unique_items")).collect()[0]["unique_items"]
# Display results
print(f'Number of policies we have : {unique_policies}')
print(f'Number of unique devices we have : {unique_imeis}')
print(f'Number of unique assets we have : {unique_items}')

%md
##### Bringing trips for the current month into the picture
from pyspark.sql.functions import to_timestamp, month, year

# Step 1: Parse start_ts and end_ts into timestamps (if not already timestamp type)
transports = transports.withColumn("start_ts_parsed", to_timestamp("start_ts"))
transports = transports.withColumn("end_ts_parsed", to_timestamp("end_ts"))

# Step 2: Filter trips 
filtered_transports = transports.filter(
    (year("start_ts_parsed") == year_1) & (month("start_ts_parsed") == month_1) &
    (year("end_ts_parsed") == year_1) & (month("end_ts_parsed") == month_1)
)

# Step 3: Join exposure_remove_canceled to filtered_transports on sentiance_id
exposure_trips = exposure_remove_canceled.join(
    filtered_transports,
    on="sentiance_id",
    how="inner"
)

# Optional: preview
exposure_trips.select(
    "policy_number", "item_no", "imei_number", "sentiance_id", "start_ts", "end_ts"
).show(10)

print(f"Total number of trips in the {month_1}th of {year_1} is : {exposure_trips.count() }")
# print(2389852-2342604)
%md
#### Aggregating trips
from pyspark.sql import functions as F

# --- Keys and feature sets ---
driver_keys   = ["sentiance_id", "policy_number", "item_no", "imei_number"]
num_features  = ["hard_accel", "hard_brake", "legal", "overall_absa_v1",
                 "distance", "trip_duration", "late_drive_duration"]
binary_feature = "late_drive"  # treated specially

# 1) Build a table of drivers that had any NULLs (per feature + total)
null_count_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
    for c in (num_features + [binary_feature])
]
drivers_with_null_values = (
    exposure_trips
    .groupBy(driver_keys)
    .agg(*null_count_exprs)
)

# Add a total_nulls column and keep only drivers who had any nulls
drivers_with_null_values = drivers_with_null_values.withColumn(
    "total_nulls",
    sum(F.col(f"{c}_nulls") for c in (num_features + [binary_feature]))
).filter(F.col("total_nulls") > 0)

# Optional: materialize for reuse / inspection
# drivers_with_null_values.write.mode("overwrite").parquet("/mnt/xxx/drivers_with_null_values")

# 2) Compute per-driver means for numeric features (ignoring nulls)
driver_means = (
    exposure_trips
    .groupBy(driver_keys)
    .agg(*[F.avg(F.col(c)).alias(f"{c}_mean") for c in num_features])
)

# 3) Compute global means for numeric features (used when a driver’s mean is null)
global_means_row = exposure_trips.agg(
    *[F.avg(F.col(c)).alias(f"{c}_global_mean") for c in num_features]
).collect()[0]
global_means = {f"{c}_global_mean": float(global_means_row[f"{c}_global_mean"] or 0.0)
                for c in num_features}

# 4) Join driver means and impute trip-level nulls
with_means = exposure_trips.join(driver_means, on=driver_keys, how="left")

# Build imputed columns for numeric features (coalesce: value -> driver_mean -> global_mean -> 0.0)
imputed_num_cols = [
    F.coalesce(
        F.col(c),
        F.col(f"{c}_mean"),
        F.lit(global_means[f"{c}_global_mean"]),
        F.lit(0.0)
    ).alias(c)
    for c in num_features
]

# Impute late_drive: any null row becomes 1
late_drive_imputed = F.when(F.col(binary_feature).isNull(), F.lit(1)).otherwise(F.col(binary_feature)).alias(binary_feature)

# Keep original non-feature columns (besides the keys) if you need them; here we reconstruct with keys + imputed features
trips_imputed = with_means.select(
    *[F.col(k) for k in driver_keys],
    *imputed_num_cols,
    late_drive_imputed
)

# 5) Aggregate on the imputed trips (your original monthly_agg logic)
monthly_agg = trips_imputed.groupBy(
    "sentiance_id", "policy_number", "item_no", "imei_number"
).agg(
    F.avg("hard_accel").alias("avg_hard_accel"),
    F.avg("hard_brake").alias("avg_hard_brake"),
    F.avg("legal").alias("avg_legal"),
    F.avg("overall_absa_v1").alias("avg_overall_absa_v1"),
    F.max("late_drive").alias("any_late_drive"),       # will be 1 if any null was present for that driver (by rule)
    F.sum("distance").alias("total_distance"),
    F.sum("trip_duration").alias("total_trip_duration"),
    F.sum("late_drive_duration").alias("total_late_drive_duration"),
    F.count("*").alias("trip_count")
)

# --- (Optional) quick sanity checks ---
print("Drivers with any nulls:", drivers_with_null_values.count())
print("Rows before:", exposure_trips.count(), " | Rows after imputation:", trips_imputed.count())
monthly_agg.select("avg_hard_accel","avg_hard_brake","avg_legal","avg_overall_absa_v1").summary("count","min","max").show()



print(f'Number of assests with trips in the {month_1}th of {year_1} : {monthly_agg.count()}')
%md
#### Finding polynomial weights and evaluating driving score
# --- Imports ---
import math
from pyspark.sql import functions as F

# =========================
# 0) Configure feature set
# =========================
# Features are already in [0,1].
# 'direction':
#   - 'bad'  -> higher is worse (we'll invert with 1 - x for scoring)
#   - 'good' -> higher is better (use as-is)
components_info = [
    {"col": "avg_hard_accel",      "direction": "bad"},
    {"col": "avg_hard_brake",      "direction": "bad"},
    {"col": "avg_legal",           "direction": "good"},
    {"col": "avg_overall_absa_v1", "direction": "good"},
    {"col": "late_drive_ratio",    "direction": "bad"},  # will create if missing
]

# =========================
# 1) Build late_drive_ratio
# =========================
if "late_drive_ratio" not in monthly_agg.columns:
    ratio_expr = F.col("total_late_drive_duration") / (F.col("total_trip_duration") + F.lit(1e-9))
    # Clamp to [0,1] using greatest/least (Spark-friendly)
    ratio_expr = F.greatest(F.lit(0.0), F.least(F.lit(1.0), ratio_expr))
    monthly_agg = monthly_agg.withColumn("late_drive_ratio", ratio_expr)

# ======================================================
# 2) Keep only rows with all components non-null for stats
# ======================================================
component_cols = [c["col"] for c in components_info]
df = monthly_agg
for c in component_cols:
    df = df.filter(F.col(c).isNotNull())

# Early exit guard: if df is empty, fallback to equal weights and zero score
if df.limit(1).count() == 0:
    # equal weights
    equal_w = 1.0 / len(component_cols)
    # aligned columns
    scored_df = monthly_agg
    for info in components_info:
        c = info["col"]
        if info["direction"] == "bad":
            scored_df = scored_df.withColumn(f"{c}__good", (F.lit(1.0) - F.col(c)))
        else:
            scored_df = scored_df.withColumn(f"{c}__good", F.col(c))
    # score
    term_sum = None
    for info in components_info:
        c = info["col"]
        term = F.lit(equal_w) * F.col(f"{c}__good")
        term_sum = term if term_sum is None else (term_sum + term)
    scored_df = scored_df.withColumn("driving_score", term_sum)
    # Update monthly_agg with driving_score
    id_cols = ["policy_number", "item_no", "imei_number", "sentiance_id"]
    monthly_agg = monthly_agg.join(scored_df.select(*id_cols, "driving_score"), on=id_cols, how="left")
else:
    # ======================================================
    # 3) Quantiles via percentile_approx (Spark-friendly)
    #    weight ∝ |mean(top20%) - mean(bottom20%)| / std_all
    # ======================================================
    def get_q(df_, colname, q):
        row = df_.select(F.expr(f"percentile_approx({colname}, {q}, 10000) as q")).collect()[0]
        return float(row["q"]) if row["q"] is not None else 0.0

    quantile_map = {}
    for c in component_cols:
        q20 = get_q(df, c, 0.20)
        q80 = get_q(df, c, 0.80)
        quantile_map[c] = (q20, q80)

    raw_weights = {}
    for c in component_cols:
        q20, q80 = quantile_map[c]

        top_df = df.filter(F.col(c) >= F.lit(q80))
        bot_df = df.filter(F.col(c) <= F.lit(q20))

        mean_top = top_df.agg(F.mean(c).alias("m")).collect()[0]["m"]
        mean_bot = bot_df.agg(F.mean(c).alias("m")).collect()[0]["m"]
        std_all  = df.agg(F.stddev_samp(c).alias("s")).collect()[0]["s"]

        mean_top = float(mean_top) if mean_top is not None else 0.0
        mean_bot = float(mean_bot) if mean_bot is not None else 0.0
        std_all  = float(std_all)  if std_all  is not None else 0.0

        denom = std_all if std_all > 1e-9 else 1e-9
        diff  = mean_top - mean_bot
        raw_weights[c] = math.fabs(diff) / denom

    # Normalize to sum to 1 (fallback equal if degenerate)
    total = sum(raw_weights.values()) if raw_weights else 0.0
    if total <= 1e-12:
        final_weights = {c: 1.0 / len(component_cols) for c in component_cols}
    else:
        final_weights = {c: (w / total) for c, w in raw_weights.items()}

    print("Learned weights (sum to 1):")
    for c in component_cols:
        print(f"  {c:25s} -> {final_weights[c]:.4f}")

    # ======================================================
    # 4) Build aligned features (convert 'bad' to 'good' via 1 - x)
    # ======================================================
    scored_df = monthly_agg
    for info in components_info:
        c = info["col"]
        if info["direction"] == "bad":
            scored_df = scored_df.withColumn(f"{c}__good", (F.lit(1.0) - F.col(c)))
        else:
            scored_df = scored_df.withColumn(f"{c}__good", F.col(c))

    # ======================================================
    # 5) Weighted sum to get driving_score
    # ======================================================
    expr_sum = None
    for info in components_info:
        c = info["col"]
        w = float(final_weights[c])
        term = F.lit(w) * F.col(f"{c}__good")
        expr_sum = term if expr_sum is None else (expr_sum + term)

    scored_df = scored_df.withColumn("driving_score", expr_sum)

    # ======================================================
    # 6) Write driving_score back into monthly_agg
    # ======================================================
    id_cols = ["policy_number", "item_no", "imei_number", "sentiance_id"]
    monthly_agg = monthly_agg.join(scored_df.select(*id_cols, "driving_score"), on=id_cols, how="left")
    print(monthly_agg.count())

# (Optional) quick peek
# display(monthly_agg.select("policy_number", "item_no", "driving_score").limit(20))

%md
##### Driving score summary statistics
from pyspark.sql import functions as F

# Minimum, mean, and maximum
stats = monthly_agg.agg(
    F.min("driving_score").alias("min_score"),
    F.mean("driving_score").alias("mean_score"),
    F.expr("percentile_approx(driving_score, 0.5)").alias("median_score"),
    F.max("driving_score").alias("max_score")
).collect()[0]

min_score = stats["min_score"]
mean_score = stats["mean_score"]
median_score = stats["median_score"]
max_score = stats["max_score"]

print(f"Min: {min_score}")
print(f"Mean: {mean_score}")
print(f"Median: {median_score}")
print(f"Max: {max_score}")

# Mode: find the most common value
mode_df = monthly_agg.groupBy("driving_score").count().orderBy(F.desc("count"))
mode_row = mode_df.first()
mode_score = mode_row["driving_score"] if mode_row else None

print(f"Mode: {mode_score}")

%md
#### Removing duplicates introduced by joining back to premiums
from pyspark.sql import functions as F, Window

JOIN_KEYS = ["policy_number", "item_no", "imei_number"]

print("monthly_agg rows (before join):", monthly_agg.count())

# --- 1) Diagnose duplicates on the right side (exposure_remove_canceled) ---
exposure_dupes = (
    exposure_remove_canceled
    .groupBy(*JOIN_KEYS)
    .count()
    .filter(F.col("count") > 1)
)

dup_ct = exposure_dupes.count()
print("Duplicated key groups in exposure_remove_canceled:", dup_ct)

if dup_ct > 0:
    # Show a few offending keys
    display(exposure_dupes.orderBy(F.col("count").desc()).limit(10))


# Fallback: arbitrarily keep the first non-null premium per key
print("Deduping exposure_remove_canceled with first() since no date priority column was found.")
# exposure_slim = (
#     exposure_remove_canceled
#     .groupBy(*JOIN_KEYS)
#     .agg(F.first("premium_collected", ignorenulls=True).alias("premium_collected"))
# )
exposure_slim = (
    exposure_remove_canceled
    .groupBy(*JOIN_KEYS)
    .agg(F.first("premium_collected", ignorenulls=True).alias("agg_premium_collected"))
)


# (Optional) sanity check: ensure uniqueness now
post_dupes = (
    exposure_slim.groupBy(*JOIN_KEYS).count().filter(F.col("count") > 1).count()
)
assert post_dupes == 0, "exposure_slim still has duplicate keys; check your dedupe logic."

# --- 3) Join using the deduped right side ---
monthly_joined = (
    monthly_agg
    .join(exposure_slim, on=JOIN_KEYS, how="left")
)

monthly_joined = monthly_joined.withColumnRenamed("agg_premium_collected", "premium_collected")

print("monthly_agg rows (after join):", monthly_joined.count())

# If you want to be strict and ensure row count didn’t inflate relative to monthly_agg:
assert monthly_joined.count() == monthly_agg.count(), \
    "Row count increased after join — indicates remaining many-to-one issues."

# Use monthly_joined going forward
monthly_agg = monthly_joined


display(monthly_agg.filter(col("policy_number") == "6555598922").filter(col("imei_number") == "868373074405053"))
%md
##### Finding number of records without driving score 
from pyspark.sql import functions as F

# Identify records with no driving score
missing_scores_df = (
    monthly_agg
    .filter(F.col("driving_score").isNull())
)

# See how many and preview
print(f"Number of records without driving_score: {missing_scores_df.count()}")
# display(missing_scores_df)

# # (Optional) Save to a CSV/Parquet for review
# output_path = "/mnt/data/missing_driving_scores"  # adjust for your mount/storage
# missing_scores_df.write.mode("overwrite").parquet(output_path)
# # Or for CSV:
# # missing_scores_df.write.mode("overwrite").option("header", True).csv(output_path)

%md
##### Finding driving scores group centers
# === Mean Shift clustering for driver_score — fully deterministic ===
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

# -----------------------------
# 0) Build a deterministic key & stable row id
# -----------------------------
# Choose a stable key (tweak to match your unique id columns)
stable_key = F.concat_ws(
    "||",
    F.col("policy_number").cast(StringType()),
    F.col("item_no").cast(StringType()),
    F.col("imei_number").cast(StringType()),
    F.col("sentiance_id").cast(StringType())
)

monthly_base = (
    monthly_agg
    .withColumn("driving_score", F.col("driving_score").cast("double"))
    .withColumn("_has_score", F.col("driving_score").isNotNull())
    .withColumn("_stable_key", stable_key)
)

# Make a stable row id using dense_rank over a deterministic order
w_id = Window.orderBy(F.col("_stable_key").asc(), F.col("driving_score").asc_nulls_last())
monthly_base = monthly_base.withColumn("_row_id", F.dense_rank().over(w_id))

scores_df = (
    monthly_base
    .filter(F.col("_has_score"))
    .select("_row_id", "_stable_key", "driving_score")
)

# -----------------------------
# 1) Deterministic ~20% sample using a stable hash
# -----------------------------
# Map xxhash64 to [0,1): take absolute value of signed 64-bit and divide by max positive
hash64 = F.xxhash64("_stable_key")
# Keep only non-negative part and normalize to [0,1]
norm = (F.col("hash64_abs") / F.lit(9_223_372_036_854_775_807))  # 2^63 - 1
sample_df = (
    scores_df
    .withColumn("hash64_abs", F.abs(hash64).cast("long"))
    .withColumn("_u", norm.cast("double"))
    .filter(F.col("_u") < F.lit(0.20))  # ~20% deterministic sample
    .orderBy(F.col("_stable_key").asc(), F.col("driving_score").asc())  # deterministic order
    .select("_row_id", "driving_score")  # keep only needed cols
)

sample_count = sample_df.count()
print(f"Deterministic sample size: {sample_count}")

# Safety cap before collect (optional, but deterministic already)
MAX_FIT_SAMPLES = 100_000
sample_df = sample_df.limit(MAX_FIT_SAMPLES)

# -----------------------------
# 2) Fit MeanShift with fixed random_state
# -----------------------------
from sklearn.cluster import MeanShift, estimate_bandwidth
import numpy as np

sample_pd = sample_df.select("driving_score").toPandas()

if sample_pd.shape[0] == 0:
    raise ValueError("No driving_score rows available to fit Mean Shift.")

# Round scores slightly to avoid microscopic float jitter across runs
values = sample_pd["driving_score"].round(8).to_numpy().reshape(-1, 1)

bandwidth = estimate_bandwidth(
    values,
    quantile=0.30,
    n_samples=min(len(values), 5000),
    random_state=42  # <- lock sklearn randomness
)

# Robust fallback if estimator returns nonpositive
if not bandwidth or bandwidth <= 0:
    std = float(np.std(values, ddof=1)) if values.shape[0] > 1 else 0.0
    n   = max(values.shape[0], 1)
    heuristic = 1.06 * std * (n ** (-1.0/5.0))  # Silverman-like 1D
    bandwidth = max(heuristic, 1e-3)

bandwidth = float(np.round(bandwidth, 8))  # normalize for display
ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
ms.fit(values)

centers_sorted = sorted(float(c) for c in ms.cluster_centers_.flatten().tolist())
print(f"Estimated bandwidth (deterministic): {bandwidth:.8f}")
print(f"Found {len(centers_sorted)} clusters (centers): {centers_sorted}")

# -----------------------------
# 3) Label by nearest center (accurate, deterministic)
# -----------------------------
from pyspark.sql.types import ArrayType

if len(centers_sorted) == 0:
    labeled_scores = scores_df.select("_row_id") \
        .withColumn("ms_cluster", F.lit(0).cast(IntegerType())) \
        .withColumn("ms_center",  F.lit(None).cast(DoubleType()))
else:
    # Tiny centers DF (deterministic order)
    centers_df = spark.createDataFrame(
        [(i, float(c)) for i, c in enumerate(centers_sorted)],
        ["ms_cluster", "ms_center"]
    ).withColumn("ms_cluster", F.col("ms_cluster").cast(IntegerType()))

    display(centers_df)

#     # Cross join and pick nearest — deterministic because both sides are ordered
#     labeled_scores = (
#         scores_df
#         .crossJoin(centers_df)
#         .withColumn("abs_diff", F.abs(F.col("driving_score") - F.col("ms_center")))
#     )
#     w = Window.partitionBy("_row_id").orderBy(F.col("abs_diff").asc(), F.col("ms_cluster").asc())
#     labeled_scores = (
#         labeled_scores
#         .withColumn("rn", F.row_number().over(w))
#         .filter(F.col("rn") == 1)
#         .select("_row_id", "ms_cluster", "ms_center")
#     )

# # -----------------------------
# # 4) Join labels back to all rows
# # -----------------------------
# monthly_labeled = (
#     monthly_base
#     .join(labeled_scores, on="_row_id", how="left")
#     .drop("_has_score", "hash64_abs", "_u")
# )

# # -----------------------------
# # 5) Optional: summaries
# # -----------------------------
# print("== Cluster profile (deterministic) ==")
# (
#     monthly_labeled
#     .groupBy("ms_cluster")
#     .agg(
#         F.count("*").alias("n"),
#         F.first("ms_center", ignorenulls=True).alias("cluster_center"),
#         F.min("driving_score").alias("min_score"),
#         F.max("driving_score").alias("max_score"),
#         F.avg("driving_score").alias("avg_score"),
#     )
#     .orderBy("ms_cluster")
# ).show(truncate=False)

%md
###### Assigning drivers to centers
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField
from pyspark.sql import Row
import builtins  # ensure we use Python's abs, not a shadowed name

# -------------------------------------------------------------------
# 0) Load/clean centers and collect to driver (tiny list)
# -------------------------------------------------------------------
centers_clean = (
    centers_df
    .select(
        F.col("ms_cluster").cast("int").alias("ms_cluster"),
        F.col("ms_center").cast("double").alias("ms_center")
    )
    .dropna()
    .dropDuplicates(["ms_cluster", "ms_center"])
    .orderBy("ms_center")
    .collect()
)

centers_list = [(int(r["ms_cluster"]), float(r["ms_center"])) for r in centers_clean]
if not centers_list:
    raise ValueError("centers_df is empty — cannot assign nearest centers.")

# -------------------------------------------------------------------
# 1) UDF that finds nearest center (absolute difference)
# -------------------------------------------------------------------
nearest_schema = StructType([
    StructField("nearest_ms_center",  DoubleType(),  True),
    StructField("ms_center_gap",      DoubleType(),  True),
    StructField("nearest_ms_cluster", IntegerType(), True),
])

def nearest_center_udf(score):
    # score arrives as a Python float or None
    if score is None:
        return Row(nearest_ms_center=None, ms_center_gap=None, nearest_ms_cluster=None)

    s = float(score)
    best_center  = None
    best_cluster = None
    best_gap     = float("inf")

    for cid, cval in centers_list:
        gap = builtins.abs(s - cval)  # true Python abs
        if gap < best_gap:
            best_gap, best_center, best_cluster = gap, cval, cid
        elif gap == best_gap and best_center is not None:
            # deterministic tie-break: pick the smaller center
            if cval < best_center:
                best_center, best_cluster = cval, cid

    return Row(nearest_ms_center=best_center,
               ms_center_gap=best_gap,
               nearest_ms_cluster=best_cluster)

nearest_udf = F.udf(nearest_center_udf, nearest_schema)

# -------------------------------------------------------------------
# 2) Apply to your data
#    - cast driving_score to double
#    - add constant bandwidth column
#    - compute nearest center fields

bandwidth_value = float(bandwidth)  # make sure it's a plain float

monthly_with_nearest = (
    monthly_agg
    .withColumn("driving_score", F.col("driving_score").cast(DoubleType()))
    .withColumn("bandwidth", F.lit(bandwidth_value))             # add constant bandwidth
    .withColumn("nearest", nearest_udf(F.col("driving_score")))  # compute nearest center info
    .withColumn("nearest_ms_center",  F.col("nearest.nearest_ms_center"))
    .withColumn("ms_center_gap",      F.col("nearest.ms_center_gap"))
    .withColumn("nearest_ms_cluster", F.col("nearest.nearest_ms_cluster"))
    .drop("nearest")
)
display(monthly_agg.limit(10))
# -------------------------------------------------------------------
# 3) Reorder columns so 'bandwidth' is IMMEDIATELY AFTER 'driving_score'
# -------------------------------------------------------------------
cols = monthly_with_nearest.columns
if "driving_score" in cols and "bandwidth" in cols:
    ds_idx = cols.index("driving_score")
    # Build a new ordered list with 'bandwidth' right after 'driving_score'
    reordered_cols = cols[:ds_idx+1] + ["bandwidth"] + [c for c in cols if c not in cols[:ds_idx+1] + ["bandwidth"]]
    monthly_with_nearest = monthly_with_nearest.select(*reordered_cols)

# -------------------------------------------------------------------
# 4) Quick sanity checks
# -------------------------------------------------------------------
# Ensure no ambiguous column references in previous operations
print("Row counts — input vs output:", monthly_agg.count(), monthly_with_nearest.count())

monthly_with_nearest.select(
    "driving_score", "bandwidth", "nearest_ms_center", "ms_center_gap", "nearest_ms_cluster"
).orderBy(F.col("ms_center_gap").asc_nulls_last()).show(5, truncate=False)

%md
#### Adding min and max driving score per group to results
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Partition by the assigned center
w = Window.partitionBy("nearest_ms_center")

monthly_with_range = (
    monthly_with_nearest
    .withColumn("min_ms_center", F.min("driving_score").over(w))
    .withColumn("max_ms_center", F.max("driving_score").over(w))
    .withColumn("no_drivers_per_group", F.count(F.lit(1)).over(w))  # group size repeated per row
)

# (Optional) quick look
monthly_with_range.select(
    "nearest_ms_center",
    "nearest_ms_cluster",
    "driving_score",
    "min_ms_center",
    "max_ms_center",
    "no_drivers_per_group"
).orderBy("nearest_ms_center", "driving_score").show(4, truncate=False)

print(monthly_with_range.count())
     

    Allocating drivers to percentages running from 5% to 30% max based on driving score centers


from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# --- Inputs ---
MIN_CB = 0.05   # 5%
MAX_CB = 0.30   # 30%
THRESH = 0.50   # groups with min_ms_center < 0.5 → 0%
ROUND_PLACES = 6  # rounding for stable join key

# Assumes monthly_with_range already has:
# nearest_ms_center, min_ms_center, max_ms_center, no_drivers_per_group, premium, ...

# 0) Ensure types + add a stable join key (rounded string). Keep the original center.
monthly_wr = (
    monthly_with_range
    .withColumn("nearest_ms_center", F.col("nearest_ms_center").cast(DoubleType()))
    .withColumn("min_ms_center",    F.col("min_ms_center").cast(DoubleType()))
    .withColumn("max_ms_center",    F.col("max_ms_center").cast(DoubleType()))
    .withColumn(
        "center_key",
        F.format_number(F.round(F.col("nearest_ms_center"), ROUND_PLACES), 6)
    )
)

# 1) One row per group, ordered by min_ms_center (keep max_ms_center too)
groups_df = (
    monthly_wr
    .select("nearest_ms_center", "min_ms_center", "max_ms_center", "center_key")
    .distinct()
    .orderBy(F.col("min_ms_center").asc())
)

# 2) Build mapping on the driver (tiny table)
rows = groups_df.collect()

bad  = [
    (float(r["nearest_ms_center"]), float(r["min_ms_center"]), r["center_key"])
    for r in rows if r["min_ms_center"] is not None and r["min_ms_center"] < THRESH
]
good = [
    (float(r["nearest_ms_center"]), float(r["min_ms_center"]), r["center_key"])
    for r in rows if r["min_ms_center"] is not None and r["min_ms_center"] >= THRESH
]

# Sort good groups by min_ms_center ascending
good.sort(key=lambda x: x[1])

mapping = []

# a) Bad groups → 0%
for center, _, ckey in bad:
    mapping.append((ckey, float(0.0), float(center)))

# b) Good groups → linearly spaced [5%, 30%]
n_good = len(good)
if n_good == 1:
    mapping.append((good[0][2], float(MAX_CB), float(good[0][0])))
elif n_good >= 2:
    span  = MAX_CB - MIN_CB
    steps = n_good - 1
    for i, (center, _, ckey) in enumerate(good):
        pct = MIN_CB + span * (i / steps)
        mapping.append((ckey, float(pct), float(center)))

map_df = (
    spark.createDataFrame(mapping, ["center_key", "cashback_percent", "map_center_sample"])
    .withColumn("center_key", F.col("center_key").cast(StringType()))
    .withColumn("cashback_percent", F.col("cashback_percent").cast(DoubleType()))
)

# 3) Join mapping back (LEFT) using the stable key; KEEP nearest_ms_center
monthly_with_percentage = (
    monthly_wr.alias("m")
    .join(map_df.alias("m2"), on="center_key", how="left")
    .drop("map_center_sample")
    .drop("center_key")
    .fillna({"cashback_percent": 0.0})
)

# 4) cashback_amount = cashback_percent * premium
monthly_with_percentage = monthly_with_percentage.withColumn(
    "cashback_amount",
    F.col("cashback_percent") * F.col("premium_collected")
)

# 5) Compute the scalar "cashback_earning_borderline"
#    Definition: max_ms_center of the group with 0% cashback that has the largest nearest_ms_center.
#    If there are no 0% groups, fallback to THRESH so the column has a value.
zero_groups_df = (
    monthly_wr
    .join(map_df, on="center_key", how="left")  # to know which groups are 0%
    .filter(F.col("cashback_percent") == 0.0)
    .select("nearest_ms_center", "max_ms_center")
    .distinct()
)

border_rows = (
    zero_groups_df
    .orderBy(F.col("nearest_ms_center").desc())
    .select("max_ms_center")
    .limit(1)
    .collect()
)

if border_rows:
    cashback_earning_borderline = float(border_rows[0]["max_ms_center"])
else:
    # No 0% groups this month — use THRESH as a sensible fallback
    cashback_earning_borderline = float(THRESH)

monthly_with_percentage = monthly_with_percentage.withColumn(
    "earning_borderline_driving_score",
    F.lit(cashback_earning_borderline)
)

# (Optional) sanity checks
monthly_with_percentage.select(
    "nearest_ms_center", "min_ms_center", "max_ms_center",
    "cashback_percent", "cashback_amount", "earning_borderline_driving_score"
).show(4, truncate=False)

# Distinct groups with their assigned % and the shared borderline
monthly_with_percentage.select(
    "nearest_ms_center", "min_ms_center", "max_ms_center", "cashback_percent", "earning_borderline_driving_score"
).distinct().orderBy("min_ms_center").show(truncate=False)

     

print(monthly_with_percentage.count())
     
Scaling

from decimal import Decimal
from pyspark.sql import functions as F

# Step 6: Aggregate totals
agg_totals = monthly_with_percentage.filter(F.col("trip_count").isNotNull()).agg(
    F.sum("premium_collected").alias("total_premium"),
    F.sum("cashback_amount").alias("total_cashback")
).collect()[0]

total_premium = agg_totals["total_premium"]
total_cashback = agg_totals["total_cashback"]

# Step 7: Calculate scaling factor
if Decimal('0.05') <= (Decimal(str(total_cashback)) / Decimal(str(total_premium))) <= Decimal('0.06'):
    scaling_factor = float(1.00)
    print(f"Scaling factor: {scaling_factor}")
else:
    
    target_cashback = total_premium * Decimal("0.05")
    scaling_factor = float(target_cashback / Decimal(str(total_cashback))) if total_cashback > 0 else 0.0

# Step 8: Apply scaling factor to cashback_amount
monthly_with_scaling = (
    monthly_with_percentage.filter(F.col("trip_count").isNotNull())
    .withColumn("scaled_cashback_amount", F.col("cashback_amount") * F.lit(scaling_factor))
    .withColumn("scaling_factor", F.lit(scaling_factor))
)

# Quick check
monthly_with_scaling.select(
    "premium_collected", "cashback_amount", "scaled_cashback_amount", "scaling_factor"
).show(10, truncate=False)


     

print(monthly_with_scaling.count())
     
Joining clients with trips to those without trip and Rearranging the data

from pyspark.sql import functions as F

all_clients = exposure_remove_canceled.select(
    "sentiance_id", "policy_number", "item_no", "imei_number", "premium_collected"
).dropDuplicates()

# Left join to monthly_with_scaling
monthly_everyone = all_clients.join(
    monthly_with_scaling.drop("premium_collected"),
    on=["sentiance_id", "policy_number", "item_no", "imei_number"],
    how="left"
)

# Desired logical order
preferred_order = [
    # IDs
    "policy_number", "item_no", "imei_number", "sentiance_id",
    # Trip features / aggregates
    "avg_hard_accel", "avg_hard_brake", "avg_legal", "avg_overall_absa_v1",
    "any_late_drive", "total_distance", "total_trip_duration",
    "total_late_drive_duration", "trip_count", "late_drive_ratio",
    # Score then borderline
    "driving_score", "earning_borderline_driving_score",
    # Clustering info
    "bandwidth","nearest_ms_cluster",
    "nearest_ms_center", "ms_center_gap",
    "min_ms_center", "max_ms_center", "no_drivers_per_group",
    # Financials (premium → % → amount → scaling → scaled)
    "premium_collected", "cashback_percent", "cashback_amount",
    "scaling_factor", "scaled_cashback_amount",
]

# Keep any extra columns (if present) at the end so nothing is lost
existing_cols = monthly_with_scaling.columns
ordered_cols = [c for c in preferred_order if c in existing_cols]
leftovers   = [c for c in existing_cols if c not in ordered_cols]

monthly_everyone_pretty = monthly_everyone.select(*(ordered_cols + leftovers))

# Flag where trip_count is null
monthly_everyone_pretty = monthly_everyone_pretty.withColumn(
    "no_trip_flag", F.when(F.col("trip_count").isNull(), 1).otherwise(0)
)

     

print(monthly_everyone_pretty.count())
display(monthly_everyone_pretty.filter(col("policy_number") == "6555598922").filter(col("imei_number") == "868373074405053"))
     
Results -> cashback as percentage of premiums before and after applying scaling factor

from pyspark.sql import functions as F

# Step 1: Calculate total premiums and total cashback paid
monthly_with_trips = monthly_everyone_pretty.filter(F.col("trip_count").isNotNull())
totals = monthly_with_trips.agg(
    F.sum("premium_collected").alias("total_premiums"),
    F.sum("cashback_amount").alias("total_cashback_paid"),
    F.sum("scaled_cashback_amount").alias("total_scaled_cashback_paid")
).collect()[0]

total_premiums = float(totals["total_premiums"])
total_cashback_paid = float(totals["total_cashback_paid"])
total_scaled_cashback_paid = float(totals["total_scaled_cashback_paid"])

cashback_pct = (total_cashback_paid / total_premiums) * 100 if total_premiums else 0
scaled_cashback_pct = (total_scaled_cashback_paid / total_premiums) * 100 if total_premiums else 0

print(f"Total Collected Premiums: {total_premiums:.2f}")
print(f"Total Cashback Paid: {total_cashback_paid:.2f}")
print(f"Cashback as % of Collected Premiums: {cashback_pct:.2f}%")  # should be ~5%
print(f"Scaled Cashback Paid: {total_scaled_cashback_paid:.2f}")
print(f"Scaled Cashback as % of Collected Premiums: {scaled_cashback_pct:.2f}%")
     
Cashback distribution

cashback_distribution = monthly_everyone_pretty.groupBy("cashback_percent").agg(
    F.count("*").alias("num_clients")
).orderBy("cashback_percent")

cashback_distribution.show()

     
Clients with no trips count

no_trip= monthly_everyone_pretty.filter(F.col("no_trip_flag") == 1)
no_trip_count = no_trip.count()

print(f"Number of clients with no trips: {no_trip_count}")

     
Preview of different groups

# Prepare the fields to show
desired_cols = monthly_everyone_pretty.columns

# cashback groups
cashback_levels = list(cashback_distribution.select("cashback_percent").filter(col("cashback_percent").isNotNull()).toPandas()["cashback_percent"].values)

for level in cashback_levels:
    print(f"\n=== Sample records for Cashback Percent: {int(level * 100)}% ===\n")

    # Filter the final_df for this cashback level
    filtered = monthly_everyone_pretty.filter(F.col("trip_count").isNotNull()).filter(
        (F.col("cashback_percent") == level)
    )

    df_level = filtered.select(*desired_cols).limit(10)

    display(df_level.toPandas())

     
Now we deal with clients that do not have trips at all for the entire month

from pyspark.sql import functions as F

# ------------------------------------------------------------------------------
# Inputs you already have:
# - monthly_everyone_pretty (has no_trip_flag, premium/premium_collected, etc.)
# - transports (trip-level with start_ts)
# - start_prev_date, start_current_date, start_next_date  (all DATE literals)
# ------------------------------------------------------------------------------

# 0) Scope: only clients present in the current cohort
final_clients = monthly_everyone_pretty.filter(F.col("no_trip_flag") == 1).select("sentiance_id").distinct()

# Normalize trip time to DATE
transports_d = transports.withColumn("trip_date", F.to_date("start_ts"))

# --- Build helper ID sets using ONLY PAST TRIPS (strictly before current month) ---
# Ever before current month
ids_ever_past_all = (
    transports_d
    .filter(F.col("trip_date") < start_current_date)
    .select("sentiance_id").distinct()
)

# Previous month only: [start_prev_date, start_current_date)
ids_prev_all = (
    transports_d
    .filter((F.col("trip_date") >= start_prev_date) & (F.col("trip_date") < start_current_date))
    .select("sentiance_id").distinct()
)

# Before previous month: (< start_prev_date)
ids_before_prev_all = (
    transports_d
    .filter(F.col("trip_date") < start_prev_date)
    .select("sentiance_id").distinct()
)

# Intersect each set with the current cohort (safety)
ids_ever_past    = final_clients.join(ids_ever_past_all, "sentiance_id", "inner")
ids_prev_only    = final_clients.join(ids_prev_all,       "sentiance_id", "inner")
ids_before_prev  = final_clients.join(ids_before_prev_all,"sentiance_id", "inner")

# === Base cohort: no trips this current month per your flag ====================
base_no_current = monthly_everyone_pretty.filter(F.col("no_trip_flag") == 1)

print("base_no_current:", base_no_current.count())

# === Categories (mutually exclusive and collectively exhaustive over base_no_current) ===

# 1) Absolutely No Trip Ever (in the past) → 0%
absolutely_no_trips = (
    base_no_current
    .join(ids_ever_past, on="sentiance_id", how="left_anti")
    .withColumn("reward_category", F.lit("Absolutely No Trip Ever"))
    .withColumn("cashback_percent", F.lit(0.00))
)

# 2) Did Not Drive This Month (Device Working) → 40%
#    No current trips (already base), but DID have trips in the previous month
recent_non_drivers = (
    base_no_current
    .join(ids_prev_only, on="sentiance_id", how="inner")
    .withColumn("reward_category", F.lit("Did Not Drive This Month (Device Working)"))
    .withColumn("cashback_percent", F.lit(0.40))
)

# 3) Had Trips Then Stopped → 5%
#    No current trips, NO prev-month trips, but had trips BEFORE prev month (past)
had_trips_then_stopped = (
    base_no_current
    .join(ids_prev_only,   on="sentiance_id", how="left_anti")  # exclude prev-month drivers
    .join(ids_before_prev, on="sentiance_id", how="inner")      # include those with older trips
    .withColumn("reward_category", F.lit("Had Trips Then Stopped"))
    .withColumn("cashback_percent", F.lit(0.05))
)

print("absolutely_no_trips:", absolutely_no_trips.count())
print("recent_non_drivers:",  recent_non_drivers.count())
print("had_trips_then_stopped:", had_trips_then_stopped.count())

# === Amounts & union ==========================================================
def add_amount(df):
    return df.withColumn("cashback_amount", F.col("premium_collected") * F.col("cashback_percent"))

absolutely_no_trips    = add_amount(absolutely_no_trips)
recent_non_drivers     = add_amount(recent_non_drivers)
had_trips_then_stopped = add_amount(had_trips_then_stopped)

no_trip_rewards = (
    absolutely_no_trips
    .unionByName(recent_non_drivers)
    .unionByName(had_trips_then_stopped)
)

print("no_trip_rewards (union):", no_trip_rewards.count())

# === Sanity: Check coverage — who fell through? ===============================
covered_ids = (
    no_trip_rewards.select("sentiance_id").distinct()
)
leftover = (
    base_no_current.select("sentiance_id").distinct()
    .join(covered_ids, on="sentiance_id", how="left_anti")
)

n_leftover = leftover.count()
print("Leftover (should be 0):", n_leftover)
if n_leftover > 0:
    # Inspect a few leftovers to see why (e.g., only future-dated trips)
    sample_leftover = leftover.limit(20)
    display(sample_leftover)

# === Final report (unchanged) =================================================
report = no_trip_rewards.select(
    "policy_number", "item_no", "imei_number", "sentiance_id",
    "reward_category", "premium_collected", "driving_score",
    "cashback_percent", "cashback_amount", "scaled_cashback_amount"
)

display(report)

# Breakdown
display(no_trip_rewards.groupBy("reward_category").count().orderBy("reward_category"))

     

from pyspark.sql.functions import sum as F_sum, col, round, count

# Group by reward_category and calculate aggregates
summary_stats = no_trip_rewards.groupBy("reward_category").agg(
    count("*").alias("client_count"),
    F_sum("premium_collected").alias("total_premiums"),
    F_sum("cashback_amount").alias("total_cashback_paid")
)
# Calculate cashback as percentage of premiums
summary_stats = summary_stats.withColumn(
    "cashback_percent_of_premium",
    round((col("total_cashback_paid") / col("total_premiums")) * 100, 2)
)



# Display the result nicely
summary_stats_pd = summary_stats.toPandas()
summary_stats_pd

     

from pyspark.sql.functions import sum as F_sum, col, round

# Aggregate total premiums and total cashback
overall_totals = no_trip_rewards.agg(
    F_sum("premium_collected").alias("total_collected_premiums"),
    F_sum("cashback_amount").alias("total_cashback_paid")
)

# Calculate cashback as % of premiums
overall_totals = overall_totals.withColumn(
    "cashback_percent_of_premium",
    round((col("total_cashback_paid") / col("total_collected_premiums")) * 100, 2)
)

# Show the result as a Pandas DataFrame
overall_totals_pd = overall_totals.toPandas()
overall_totals_pd

     

this_month_premium = total_premiums + float(overall_totals_pd.loc[0, "total_collected_premiums"])
this_month_payment = total_scaled_cashback_paid + float(overall_totals_pd.loc[0, "total_cashback_paid"])
cashback_percentage = (this_month_payment / this_month_premium)*100
print(f"This month premiums collected : {this_month_premium:.2f}")
print(f"This month cashback paid : {this_month_payment:.2f}")
print(f"This month cashback as % of premiums : { cashback_percentage:.2f}%")
     
Combining results from non trip clients to trip clients

from pyspark.sql import functions as F

# 0) Desired output columns in the exact order you want


# 1) Trips side (has real scaled_cashback_amount already)
trip_selected = (
    monthly_everyone_pretty
    .filter(F.col("trip_count").isNotNull())
    .select(*[F.col(c) for c in desired_cols])
    .withColumn("reward_category", F.lit("Had Trips"))
)

# 2) Build the no-trip selection to match schema & order dynamically
trip_schema_by_name = {f.name: f.dataType for f in trip_selected.schema.fields}
no_trip_cols = set(no_trip_rewards.columns)

no_trip_exprs = []
for c in desired_cols:
    dtype = trip_schema_by_name[c]

    if c == "scaled_cashback_amount":
        # Special rule: scaled = cashback_amount for no-trip rows
        if "cashback_amount" in no_trip_cols:
            no_trip_exprs.append(F.col("cashback_amount").cast(dtype).alias(c))
        else:
            no_trip_exprs.append(F.lit(None).cast(dtype).alias(c))
    elif c in no_trip_cols:
        no_trip_exprs.append(F.col(c).cast(dtype).alias(c))
    else:
        no_trip_exprs.append(F.lit(None).cast(dtype).alias(c))

# Prefer the category from no_trip_rewards if present, else default
reward_cat_expr = (
    F.col("reward_category")
    if "reward_category" in no_trip_cols
    else F.lit("No Trips")
).alias("reward_category")

no_trip_selected = no_trip_rewards.select(*no_trip_exprs, reward_cat_expr)

# 3) Union (schemas and order match perfectly)
all_clients_results = trip_selected.unionByName(no_trip_selected)

# (Optional) quick sanity check
print("Trips rows:", trip_selected.count())
print("No-trip rows:", no_trip_selected.count())
print("Combined rows:", all_clients_results.count())
# display(all_clients_results.limit(20))

     
Adding charged premium

from pyspark.sql import functions as F

# Join keys
keys = ["policy_number", "item_no", "imei_number"]

# 1) Prepare the source fields from exposure_remove_canceled
exposure_src = exposure_remove_canceled.select(*keys, "premium", "item_description","original_lob_asset_id")

# 2) Drop any existing premium or item_description in all_clients_results (to avoid duplicates)
base = all_clients_results
for colname in ["premium", "item_description", "original_lob_asset_id"]:
    if colname in base.columns:
        base = base.drop(colname)

# 3) Left-join to add premium and item_description
all_clients_enriched = base.join(exposure_src, on=keys, how="left")

# 4) Reorder columns:
cols = all_clients_enriched.columns

# Move `item_description` right after `item_no`
if "item_description" in cols and "item_no" in cols:
    idx_item_no = cols.index("item_no")
    # Place item_description right after item_no
    cols.remove("item_description")
    cols = cols[:idx_item_no+1] + ["item_description"] + cols[idx_item_no+1:]

# Move `premium` just before `premium_collected`
if "premium" in cols and "premium_collected" in cols:
    cols.remove("premium")
    idx_premcol = cols.index("premium_collected")
    cols = cols[:idx_premcol] + ["premium"] + cols[idx_premcol:]

# Move `"original_lob_asset_id"` just before `item_no`
if "original_lob_asset_id" in cols and "item_no" in cols:
    cols.remove("original_lob_asset_id")
    idx_premcol = cols.index("item_no")
    cols = cols[:idx_premcol] + ["original_lob_asset_id"] + cols[idx_premcol:]

# Select in the final order
all_clients_with_premium = all_clients_enriched.select(*cols)

# Rename column premium -> premium_charged
all_clients_with_premium = all_clients_with_premium.withColumnRenamed("premium", "premium_charged")

# Round premium_collected to 2 decimal places
all_clients_with_premium = all_clients_with_premium.withColumn(
    "premium_collected", F.round(F.col("premium_collected"), 2)
)

# Optional: sanity check
display(all_clients_with_premium.limit(2))


# (Optional) sanity checks
print("Rows before join:", all_clients_results.count())
print("Rows after  join:", all_clients_with_premium.count())
# display(all_clients_enriched.limit(10))

     

from pyspark.sql.functions import avg, sum as F_sum, max as F_max

agg_df = all_clients_with_premium.agg(
    F_sum("premium_charged").alias("premium_char"),
    F_sum("premium_collected").alias("premium_coll"),

)
result = agg_df.collect()[0]
print("Sum of premiums charged:", result["premium_char"])
print("Sum of premiums collected:", result["premium_coll"])

     
Thresholding the amounts

from pyspark.sql.functions import col, round as F_round, when  #fields_to_show

# Create cycle_reward_thresh directly
all_clients_with_premium_1 = all_clients_with_premium.withColumn(
    "cycle_reward_thresh",
    when(F_round(col("scaled_cashback_amount")) < 10, 0)
    .when((F_round(col("scaled_cashback_amount")) >= 10) & (F_round(col("scaled_cashback_amount")) < 20), 20)
    .otherwise(F_round(col("scaled_cashback_amount")))
)

display(all_clients_with_premium_1.filter(col("no_trip_flag") == "1").limit(300)
)
     

from pyspark.sql import functions as F

df = (
    all_clients_with_premium_1
    # ensure numeric types (in case of schema drift)
    .withColumn("premium_collected_d", F.col("premium_collected").cast("double"))
    .withColumn("cycle_reward_thresh_d", F.col("cycle_reward_thresh").cast("double"))
)

# Bucket clients:
# - Earn > 0: final amount to be paid is positive
# - Zero (no premium_collected): zero payout because premium_collected is 0 or null
# - Zero (model 0%): zero payout despite positive premium_collected (e.g., model assigned 0%)
dist = (
    df.withColumn(
        "payout_bucket",
        F.when(F.col("cycle_reward_thresh_d") > 0, F.lit("Earn > 0"))
         .when(F.coalesce(F.col("premium_collected_d"), F.lit(0.0)) <= 0, F.lit("Zero — no premium_collected"))
         .otherwise(F.lit("Zero — model 0%"))
    )
    .groupBy("payout_bucket")
    .agg(
        F.count("*").alias("num_clients"),
        F.min("premium_collected_d").alias("min_premium_collected"),
        F.max("premium_collected_d").alias("max_premium_collected"),
        F.min("cycle_reward_thresh_d").alias("min_cycle_reward_thresh"),
        F.max("cycle_reward_thresh_d").alias("max_cycle_reward_thresh")
    )
    .orderBy("payout_bucket")
)

dist.show(truncate=False)

     

zero_breakdown = (
    df.filter(F.col("cycle_reward_thresh_d") <= 0)
      .groupBy(
          F.when(F.coalesce(F.col("premium_collected_d"), F.lit(0.0)) <= 0,
                 F.lit("Zero due to no premium_collected")
          ).otherwise(F.lit("Zero due to model 0%")).alias("reason")
      )
      .agg(F.count("*").alias("num_clients"))
)

zero_breakdown.show(truncate=False)

     

from pyspark.sql.functions import min as F_min

agg_df = all_clients_with_premium_1.agg(
    F_min("premium_collected").alias("min_premium_collected")
)
result = agg_df.collect()[0]
print("mim premium collected:", result["min_premium_collected"])
     

print(all_clients_with_premium_1.filter(col("premium_collected") < "0.00").count())
     

cashback_distribution1 = all_clients_with_premium_1.groupBy("cashback_percent").agg(
    F.count("*").alias("num_clients")
).orderBy("cashback_percent")

cashback_distribution1.show()


     

print(all_clients_with_premium_1.filter(col("premium_collected") == "0").count())
     
Adding dates to the results

from datetime import date
import calendar
from pyspark.sql.functions import lit

# === INPUT: Month and (optional) year ===
input_month = month_1           # e.g., February
input_year = date.today().year  # Defaults to current year (e.g., 2025)
input_year = datetime.strptime(reward_calculation_date, "%Y-%m-%d").year

# === Step 1: Calculate first and fourth-last day of the input month ===
first_day = date(input_year, input_month, 1).strftime("%Y-%m-%d")
last_day_of_month = calendar.monthrange(input_year, input_month)[1]
fourth_last_day = date(input_year, input_month, last_day_of_month - 3).strftime("%Y-%m-%d")

# === Step 2: Add 3 full months ===
future_month = input_month + 3
future_year = input_year

if future_month > 12:
    future_month -= 12
    future_year += 1

future_last_day = calendar.monthrange(future_year, future_month)[1]
maturation_day = date(future_year, future_month, future_last_day).strftime("%Y-%m-%d")

# === Step 3: Add to DataFrame ===
all_clients_with_premium_2 = all_clients_with_premium_1 \
    .withColumn("cycle_start_date", lit(first_day)) \
    .withColumn("cycle_end_date", lit(fourth_last_day)) \
    .withColumn("cycle_maturation_date", lit(maturation_day))

# === Optional: Print for confirmation ===
print("cycle_start_date:", first_day)
print("cycle_end_date:", fourth_last_day)
print("cycle_maturation_date:", maturation_day)

     

display(all_clients_with_premium_2)
     
Adding calculated datetime

from pyspark.sql.types import TimestampType# Add a column with the same timestamp for all rows (when cell is executed)

current_ts = datetime.now()

all_clients_with_premium_3 = all_clients_with_premium_2.withColumn("calculated_datetime", F.lit(current_ts).cast(TimestampType()))
     
Separating clients with collected premium zero from those with collected premium greater than zero

all_clients_with_premium_collected_greater_than_zero = all_clients_with_premium_3.filter(col("premium_collected") > "1.84")
all_clients_with_premium_collected_less_or_equals_to_zero = all_clients_with_premium_3.filter(col("premium_collected") <= "1.84")
print(f"Clients with collected_premiums less or equals to zero: {all_clients_with_premium_collected_less_or_equals_to_zero.count()}")
display(all_clients_with_premium_collected_less_or_equals_to_zero.limit(10))
print(f"Clients with collected_premiums greater than zero: {all_clients_with_premium_collected_greater_than_zero.count()}")
display(all_clients_with_premium_collected_greater_than_zero.limit(10))

     

from pyspark.sql import functions as F

df = (
    all_clients_with_premium_collected_greater_than_zero
    # ensure numeric types (in case of schema drift)
    .withColumn("premium_collected_d", F.col("premium_collected").cast("double"))
    .withColumn("cycle_reward_thresh_d", F.col("cycle_reward_thresh").cast("double"))
)

# Bucket clients:
# - Earn > 0: final amount to be paid is positive
# - Zero (no premium_collected): zero payout because premium_collected is 0 or null
# - Zero (model 0%): zero payout despite positive premium_collected (e.g., model assigned 0%)
dist = (
    df.withColumn(
        "payout_bucket",
        F.when(F.col("cycle_reward_thresh_d") > 0, F.lit("Earn > 0"))
         .when(F.coalesce(F.col("premium_collected_d"), F.lit(0.0)) <= 0, F.lit("Zero — no premium_collected"))
         .otherwise(F.lit("Zero — model 0%"))
    )
    .groupBy("payout_bucket")
    .agg(
        F.count("*").alias("num_clients"),
        F.min("premium_collected_d").alias("min_premium_collected"),
        F.max("premium_collected_d").alias("max_premium_collected"),
        F.min("cycle_reward_thresh_d").alias("min_cycle_reward_thresh"),
        F.max("cycle_reward_thresh_d").alias("max_cycle_reward_thresh")
    )
    .orderBy("payout_bucket")
)

dist.show(truncate=False)

     
Adding cycle number into results

from pyspark.sql import functions as F

# Ensure cycle_end_date is treated as a date
df = all_clients_with_premium_collected_greater_than_zero.withColumn(
    "cycle_end_date", F.to_date("cycle_end_date")
)

# Step 1: Compute month index for cycle_end_date
df = df.withColumn("month_idx", F.year("cycle_end_date") * 12 + F.month("cycle_end_date"))

# Step 2: Compute base index (for August 2025 → cycle_number = 80)
# August 2025 corresponds to year=2025, month=8 → base index = 2025*12 + 8
base_year, base_month, base_number = 2025, 8, 80
base_idx = base_year * 12 + base_month

# Step 3: Calculate cycle_number relative to base_idx
df = df.withColumn(
    "cycle_number",
    F.col("month_idx") - F.lit(base_idx) + F.lit(base_number)
)

# Step 4: Drop helper column
df = df.drop("month_idx")

# Final DataFrame
all_clients_with_premium_collected_greater_than_zero_1 = df

# Quick check
display(all_clients_with_premium_collected_greater_than_zero_1.limit(3))

     

# from pyspark.sql import functions as F

# # Ensure cycle_end_date is treated as a date
# df = all_clients_with_premium_collected_less_or_equals_to_zero.withColumn(
#     "cycle_end_date", F.to_date("cycle_end_date")
# )

# # Step 1: Compute month index for cycle_end_date
# df = df.withColumn("month_idx", F.year("cycle_end_date") * 12 + F.month("cycle_end_date"))

# # Step 2: Compute base index (for August 2025 → cycle_number = 80)
# # August 2025 corresponds to year=2025, month=8 → base index = 2025*12 + 8
# base_year, base_month, base_number = 2025, 8, 80
# base_idx = base_year * 12 + base_month

# # Step 3: Calculate cycle_number relative to base_idx
# df = df.withColumn(
#     "cycle_number",
#     F.col("month_idx") - F.lit(base_idx) + F.lit(base_number)
# )

# # Step 4: Drop helper column
# df = df.drop("month_idx")

# # Final DataFrame
# all_clients_with_premium_collected_less_or_equals_to_zero_1 = df

# # Quick check
# display(all_clients_with_premium_collected_less_or_equals_to_zero_1.limit(3))
     
Reporting results

from pyspark.sql import functions as F

df = (
    all_clients_with_premium_collected_greater_than_zero_1
    # ensure numeric types (in case of schema drift)
    .withColumn("premium_collected_d", F.col("premium_collected").cast("double"))
    .withColumn("cycle_reward_thresh_d", F.col("cycle_reward_thresh").cast("double"))
)

# Bucket clients:
# - Earn > 0: final amount to be paid is positive
# - Zero (no premium_collected): zero payout because premium_collected is 0 or null
# - Zero (model 0%): zero payout despite positive premium_collected (e.g., model assigned 0%)
dist = (
    df.withColumn(
        "payout_bucket",
        F.when(F.col("cycle_reward_thresh_d") > 0, F.lit("Earn > 0"))
         .when(F.coalesce(F.col("premium_collected_d"), F.lit(0.0)) <= 0, F.lit("Zero — no premium_collected"))
         .otherwise(F.lit("Zero — model 0%"))
    )
    .groupBy("payout_bucket")
    .agg(
        F.count("*").alias("num_clients"),
        F.min("premium_collected_d").alias("min_premium_collected"),
        F.max("premium_collected_d").alias("max_premium_collected"),
        F.min("cycle_reward_thresh_d").alias("min_cycle_reward_thresh"),
        F.max("cycle_reward_thresh_d").alias("max_cycle_reward_thresh")
    )
    .orderBy("payout_bucket")
)

dist.show(truncate=False)

     
Creating the table

# from pyspark.sql.types import (
#     StructType, StructField,
#     StringType, DoubleType, IntegerType, DateType, TimestampType
# )

# # ---- Table name ----
# table_name = "prod_activate_telematics_monthly_incomplete_rewards"

# # ---- Desired schema (columns in the exact order you provided) ----
# schema = StructType([
#     StructField("policy_number",          StringType(),   False),
#     StructField("original_lob_asset_id",  StringType(),   True),
#     StructField("item_no",                StringType(),   False),
#     StructField("item_description",       StringType(),   True),
#     StructField("imei_number",            StringType(),   True),
#     StructField("sentiance_id",           StringType(),   True),

#     StructField("avg_hard_accel",         DoubleType(),   True),
#     StructField("avg_hard_brake",         DoubleType(),   True),
#     StructField("avg_legal",              DoubleType(),   True),
#     StructField("avg_overall_absa_v1",    DoubleType(),   True),
#     StructField("any_late_drive",         StringType(),   True),   # kept as String for compatibility

#     StructField("total_distance",         DoubleType(),   True),
#     StructField("total_trip_duration",    DoubleType(),   True),
#     StructField("total_late_drive_duration", DoubleType(), True),
#     StructField("trip_count",             IntegerType(),  True),
#     StructField("late_drive_ratio",       DoubleType(),   True),

#     StructField("driving_score",          DoubleType(),   True),
#     StructField("earning_borderline_driving_score", DoubleType(), True),
#     StructField("bandwidth",              DoubleType(),   True),

#     StructField("nearest_ms_cluster",     IntegerType(),  True),
#     StructField("nearest_ms_center",      DoubleType(),   True),
#     StructField("ms_center_gap",          DoubleType(),   True),
#     StructField("min_ms_center",          DoubleType(),   True),
#     StructField("max_ms_center",          DoubleType(),   True),
#     StructField("no_drivers_per_group",   IntegerType(),  True),

#     StructField("premium_charged",        DoubleType(),   True),
#     StructField("premium_collected",      DoubleType(),   True),

#     StructField("cashback_percent",       DoubleType(),   True),
#     StructField("cashback_amount",        DoubleType(),   True),
#     StructField("scaling_factor",         DoubleType(),   True),
#     StructField("scaled_cashback_amount", DoubleType(),   True),

#     StructField("no_trip_flag",           IntegerType(),  True),
#     StructField("reward_category",        StringType(),   True),
#     StructField("cycle_reward_thresh",    DoubleType(),   True),

#     StructField("cycle_start_date",       DateType(),     True),
#     StructField("cycle_end_date",         DateType(),     True),
#     StructField("cycle_maturation_date",  DateType(),     True),

#     StructField("calculated_datetime",    TimestampType(), True),
#     StructField("cycle_number",           IntegerType(),  False),
# ])

# # ---- Create an empty DataFrame with the schema ----
# empty_df = spark.createDataFrame([], schema)

# # ---- (Re)create the Delta table with this schema ----
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# (
#     empty_df.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(table_name)
# )

# print(f"✅ Delta table `{table_name}` recreated with the requested schema and column order.")

     
Writing results to prod_monthly_rewards table

print(all_clients_with_premium_collected_greater_than_zero_1.count())
# print(all_clients_with_premium_collected_less_or_equals_to_zero_1.count())
     
Writing rewards results ensuring no duplication cycle for a given policy number and item number combination

from delta.tables import DeltaTable

# Load the target Delta table
monthly_rewards_tbl = DeltaTable.forName(spark, "prod_activate_telematics_monthly_rewards")

# Merge final_result data, ensuring duplicates (policy_number, item_no, cycle_number) are not inserted
monthly_rewards_tbl.alias("t").merge(
    all_clients_with_premium_collected_greater_than_zero_1.alias("s"),  # <- here we reference your final_result DataFrame
    "t.policy_number = s.policy_number AND t.item_no = s.item_no AND t.cycle_number = s.cycle_number"
).whenNotMatchedInsertAll().execute()


     

# from delta.tables import DeltaTable

# # Load the target Delta table
# monthly_rewards_tbl = DeltaTable.forName(spark, "prod_activate_telematics_monthly_incomplete_rewards")

# # Merge final_result data, ensuring duplicates (policy_number, item_no, cycle_number) are not inserted
# monthly_rewards_tbl.alias("t").merge(
#     all_clients_with_premium_collected_less_or_equals_to_zero_1.alias("s"),  # <- here we reference your final_result DataFrame
#     "t.policy_number = s.policy_number AND t.item_no = s.item_no AND t.cycle_number = s.cycle_number"
# ).whenNotMatchedInsertAll().execute()

     
Deleting table or records in a table

# Deleting all records in the table
# spark.sql("TRUNCATE TABLE prod_activate_telematics_monthly_rewards")
# Deleting the whole table
# spark.sql("DROP TABLE prod_activate_telematics_monthly_incomplete_rewards")


     
