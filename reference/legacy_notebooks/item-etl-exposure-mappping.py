import time
from datetime import datetime
import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, regexp_extract, regexp_replace



# === Configuration ===
job_name = "telematics_policy_imei_mapping_etl"
run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
target_table = "`aws-edla-nonprod-group-catalog`.edla_dev_telm_za_publish.telematics_policy_device_mapping_exposure"

# === MLflow Setup ===
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Shared/aic_etl")

try:
    with mlflow.start_run(run_name=job_name):
        start_time = time.time()

        # Log job parameters
        mlflow.log_param("job_name", job_name)
        mlflow.log_param("run_date", run_date)
        mlflow.set_tag("pipeline_stage", "ETL")

        # ====================================================================================
        # 📥 EXTRACT
        # ====================================================================================
        sdf_telematic_pdd_exposure = spark.sql("SELECT * FROM `aws-edla-nonprod-group-catalog`.edla_dev_telm_za_publish.aq_policy_device_details")
        sdf_telematic_user_imei = spark.sql("SELECT * FROM `aws-edla-nonprod-group-catalog`.edla_dev_telm_za_publish.aq_user_imei")
        sdf_mi_pdd_exposure = spark.sql("SELECT * FROM `aws-edla-nonprod-group-catalog`.edla_dev_telm_za_publish.aq_activateitempremiumexposure")

        mlflow.log_metric("record_count_pdd_telematics", sdf_telematic_pdd_exposure.count())
        mlflow.log_metric("record_count_user_imei", sdf_telematic_user_imei.count())
        mlflow.log_metric("record_count_mi", sdf_mi_pdd_exposure.count())

        # ====================================================================================
        # 🔁 TRANSFORM PIPELINE: Merge MI and Telematics Data with Clean Features
        # ====================================================================================

        # STEP 1: Rename columns for consistency and downstream compatibility
        rename_cols = {
        "SourceData": "source_data", "PolicyNo": "policy_no", "PolicyNumber": "policy_number",
        "PolicyInceptionDate": "policy_inception_date", "ItemInceptionDate": "item_inception_date",
        "CancelledDate": "cancelled_date", "EFF_FROM_DATE": "eff_from_date", "EFF_TO_DATE": "eff_to_date",
        "SectionCode": "section_code", "CoverCode": "cover_code", "ItemDescription": "item_description",
        "VehMake": "veh_make", "VehModel": "veh_model", "Registration": "registration",
        "VinNumber": "vin_number", "IMEINumber": "imei_number", "CoverPremium": "cover_premium",
        "CoverSumInsured": "cover_sum_insured", "CoverDescription": "cover_description",
            "PolicyStatus": "policy_status", "CoverStatus": "cover_status", "LOB_ASSET_ID": "lob_asset_id",
            "ItemNo": "item_no", "PolicyCancellationReasonCode": "policy_cancellation_reason_code",
        "enddate": "end_date", "effectivefromdate": "effective_from_date", "effectivetodate": "effective_to_date",
        "EffectiveYearMonth": "effective_year_month", "EffectiveYear": "effective_year",
        "EffectiveQuarter": "effective_quarter", "EffectiveMonth": "effective_month",
        "EffectiveYearQuarter": "effective_year_quarter", "ItemCancelledDate": "item_cancelled_date"
        ,"PremiumPaid":"premium_collected","ACTIVE_LOB_ASSET_ID":"active_lob_asset_id","original_lob_asset_id":"original_lob_asset_id"
        }

        # Apply renaming on MI data
        sdf_mi_exsposure_renamed = sdf_mi_pdd_exposure
        for old_name, new_name in rename_cols.items():
            if old_name in sdf_mi_exsposure_renamed.columns:
                sdf_mi_exsposure_renamed = sdf_mi_exsposure_renamed.withColumnRenamed(old_name, new_name)

        # STEP 2: Clean and derive features from policy_no
        sdf_mi_cleaned = (
        sdf_mi_exsposure_renamed
        .withColumn("cleaned_policy_no", regexp_replace("policy_no", "-", ""))
        .withColumn("policy_product_description", regexp_extract("cleaned_policy_no", r"^([A-Za-z]+)", 1))
        .withColumn("policy_version_number", F.split(col("policy_no"), "/").getItem(1).cast("int"))
        )

        # STEP 3: Keep latest policy version per policy_number, lob_asset_id, and month
        window_latest_mi = Window.partitionBy("policy_number", "lob_asset_id", "effective_year_month") \
                                .orderBy(F.col("policy_version_number").desc())

        sdf_mi_latest = (
        sdf_mi_cleaned
        .withColumn("rn", F.row_number().over(window_latest_mi))
        .filter(col("rn") == 1)
        .drop("rn")
        )

        # STEP 4: Keep latest telematics record per policy_number and item_no
        window_latest_telematics = Window.partitionBy("policy_number", "item_no") \
                                        .orderBy(F.col("inserted_datetime").desc())

        sdf_telematics_latest = (
        sdf_telematic_pdd_exposure
        .withColumn("row_num", F.row_number().over(window_latest_telematics))
        .filter(col("row_num") == 1)
        .drop("row_num", "row_id")  # Drop helper columns
        )

        # STEP 5: Get earliest detection date per IMEI
        sdf_imei_first_use = (
        sdf_telematic_user_imei
        .groupBy("imei_number")
        .agg(F.min("retrieved_datetime").alias("imei_first_detection_datetime"))
        )

        # STEP 6: Correct installation_datetime using first detection date
        sdf_telematics_corrected = (
        sdf_telematics_latest.alias("te")
        .join(sdf_imei_first_use.alias("ui"), on="imei_number", how="left")
        .withColumn(
            "installation_datetime_final",
            when(col("te.installation_datetime").isNull(), col("ui.imei_first_detection_datetime"))
            .when(col("ui.imei_first_detection_datetime") < col("te.installation_datetime"), col("ui.imei_first_detection_datetime"))
            .otherwise(col("te.installation_datetime"))
        )
        )

        # STEP 7: Join MI and Telematics data on lob_asset_id vs item_no
        sdf_mi_telematics_combined = (
        sdf_telematics_corrected.alias("telematics")
        .join(
            sdf_mi_latest.alias("mi"),
            on=col("telematics.item_no") == col("mi.lob_asset_id"),
            how="right"
        )
        )

        # STEP 8: Final Selection and Column Ordering
        sdf_mi_telematics_combined_final = sdf_mi_telematics_combined.select(
        # Metadata
        F.col("mi.source_data").alias("source_data"),

        # Policy info
        F.col("mi.policy_version_number"),
        F.col("mi.policy_product_description"),
        F.col("mi.policy_number"),

        # Item info
        F.col("mi.lob_asset_id").alias("item_no"),
        F.col("mi.cover_premium").alias("premium"),
        F.col("mi.premium_collected"),
        F.col("mi.cover_sum_insured"),
        F.col("mi.active_lob_asset_id"),
        F.col("mi.ItemNo").alias("original_lob_asset_id"),

        # Vehicle info
        F.col("mi.vin_number"),
        F.col("mi.imei_number"),
        F.col("mi.veh_make").alias("make"),
        F.col("mi.veh_model").alias("model"),
        F.col("mi.registration").alias("registration_number"),

        # Section info
        F.col("mi.section_code"),
        F.col("mi.cover_code"),
        F.col("mi.item_description"),
        F.col("mi.cover_description"),

        # Status fields
        F.col("mi.policy_status"),
        F.col("mi.cover_status"),
        F.col("mi.policy_cancellation_reason_code"),

        # Dates
        F.col("mi.item_inception_date").alias("item_inception_datetime"),
        F.col("mi.policy_inception_date"),
        F.col("mi.item_cancelled_date").alias("item_cancellation_date"),
        F.col("mi.cancelled_date").alias("policy_cancellation_date"),
        F.col("telematics.imei_first_detection_datetime"),
        F.col("telematics.installation_datetime").alias("installation_datetime"),

        # Exposure periods
        F.col("mi.effective_year_month"),
        F.col("mi.effective_from_date"),
        F.col("mi.effective_to_date"),

        # Ingestion metadata
        F.current_timestamp().alias("inserted_timestamp")
        )
        mlflow.log_metric("record_count_final", sdf_mi_telematics_combined_final.count())

        # ====================================================================================
        # 📤 LOAD
        # ====================================================================================

        sdf_mi_telematics_combined_final.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("inserted_timestamp") \
            .saveAsTable(target_table)

        mlflow.set_tag("target_table", target_table)
        mlflow.set_tag("status", "success")

        # ====================================================================================
        # ✅ DONE
        # ====================================================================================
        duration = round(time.time() - start_time, 2)
        mlflow.log_metric("etl_duration_seconds", duration)
        print(f"ETL job '{job_name}' completed in {duration} seconds.")

except Exception as e:
    mlflow.set_tag("status", "failed")
    mlflow.log_param("error_message", str(e))
    print(f"ETL job '{job_name}' failed with error: {str(e)}")
    raise