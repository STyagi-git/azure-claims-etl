# Databricks notebook source
# COMMAND ----------
# Silver Transform: Clean + dedupe + conformed table (delta)

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import dbutils

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("bronze_input_path", "abfss://bronze@styagidatalake.dfs.core.windows.net/claims_delta/")
dbutils.widgets.text("silver_output_path", "abfss://silver@styagidatalake.dfs.core.windows.net/claims_silver/")
dbutils.widgets.text("process_from_date", "2022-01-01")

bronze_input_path = dbutils.widgets.get("bronze_input_path")
silver_output_path = dbutils.widgets.get("silver_output_path")
process_from_date = dbutils.widgets.get("process_from_date")

bronze = spark.read.format("delta").load(bronze_input_path)

bronze_inc = bronze.filter(F.col("ClaimDate") >= F.to_date(F.lit(process_from_date)))

clean = (bronze_inc
         .filter(F.col("ClaimID").isNotNull())
         .filter(F.col("ClaimDate").isNotNull())
         .filter(F.col("ClaimAmount").isNotNull())
         .filter(F.col("ClaimAmount") >= 0))

w = Window.partitionBy("ClaimID").orderBy(F.col("_ingested_at").desc())
deduped = (clean
           .withColumn("_rn", F.row_number().over(w))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

text_cols = ["PatientGender","ProviderSpecialty","ClaimStatus","PatientMaritalStatus","PatientEmploymentStatus","ClaimType","ClaimSubmissionMethod"]
for c in text_cols:
    deduped = deduped.withColumn(c, F.initcap(F.trim(F.col(c))))

deduped = deduped.withColumn("claim_year", F.year("ClaimDate")).withColumn("claim_month", F.month("ClaimDate"))

(deduped.write
    .format("delta")
    .mode("append")
    .partitionBy("claim_year","claim_month")
    .save(silver_output_path))

print(f"Wrote Silver Delta to: {silver_output_path}")
