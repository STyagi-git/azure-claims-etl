# Databricks notebook source
# COMMAND ----------
# Gold Marts: KPI aggregations + simple fraud signals

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import dbutils

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("silver_input_path", "abfss://silver@styagidatalake.dfs.core.windows.net/claims_silver/")
dbutils.widgets.text("gold_output_path", "abfss://gold@styagidatalake.dfs.core.windows.net/marts/")

silver_input_path = dbutils.widgets.get("silver_input_path")
gold_output_path = dbutils.widgets.get("gold_output_path")

claims = spark.read.format("delta").load(silver_input_path)

kpi_daily = (claims
             .groupBy(F.to_date("ClaimDate").alias("claim_date"),
                      "ProviderSpecialty",
                      "ClaimStatus",
                      "ClaimType")
             .agg(F.countDistinct("ClaimID").alias("claim_count"),
                  F.sum("ClaimAmount").alias("total_claim_amount"),
                  F.avg("ClaimAmount").alias("avg_claim_amount"),
                  F.expr("percentile_approx(ClaimAmount, 0.5)").alias("median_claim_amount")))

(kpi_daily.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(f"{gold_output_path}/mart_claims_kpi_daily"))

fraud = (claims
         .withColumn("flag_high_amount", (F.col("ClaimAmount") >= 9000).cast("int"))
         .withColumn("flag_young_inpatient", ((F.col("PatientAge") <= 5) & (F.col("ClaimType")=="Inpatient")).cast("int"))
         .withColumn("fraud_score", F.col("flag_high_amount")*2 + F.col("flag_young_inpatient"))
         .select("ClaimID","ClaimDate","ProviderID","ProviderSpecialty","ClaimType","ClaimAmount","PatientAge",
                 "fraud_score","flag_high_amount","flag_young_inpatient"))

(fraud.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(f"{gold_output_path}/mart_fraud_signals"))

print("Gold marts written.")
