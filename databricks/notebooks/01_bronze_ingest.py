# Databricks notebook source
# Bronze Ingest: Claims CSV -> Delta (ADLS Gen2)

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark import dbutils

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("raw_input_path", "abfss://raw@styagidatalake.dfs.core.windows.net/claims/")
dbutils.widgets.text("bronze_output_path", "abfss://bronze@styagidatalake.dfs.core.windows.net/claims_delta/")
dbutils.widgets.text("ingest_date", datetime.utcnow().strftime("%Y-%m-%d"))

raw_input_path = dbutils.widgets.get("raw_input_path")
bronze_output_path = dbutils.widgets.get("bronze_output_path")
ingest_date = dbutils.widgets.get("ingest_date")

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("mode", "PERMISSIVE")
      .csv(raw_input_path))

df2 = (df
       .withColumn("ClaimAmount", F.col("ClaimAmount").cast("double"))
       .withColumn("PatientAge", F.col("PatientAge").cast("int"))
       .withColumn("PatientIncome", F.col("PatientIncome").cast("double"))
       .withColumn("ClaimDate", F.to_date("ClaimDate", "dd-MM-yyyy"))
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_ingest_date", F.lit(ingest_date))
       .withColumn("_source_file", F.input_file_name()))

hash_cols = ["ClaimID","PatientID","ProviderID","ClaimAmount","ClaimDate","DiagnosisCode","ProcedureCode",
             "PatientAge","PatientGender","ProviderSpecialty","ClaimStatus","PatientIncome","PatientMaritalStatus",
             "PatientEmploymentStatus","ProviderLocation","ClaimType","ClaimSubmissionMethod"]

df2 = df2.withColumn("_record_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256))

(df2.write
    .format("delta")
    .mode("append")
    .partitionBy("_ingest_date")
    .save(bronze_output_path))

print(f"Wrote Bronze Delta to: {bronze_output_path} (partition _ingest_date={ingest_date})")
