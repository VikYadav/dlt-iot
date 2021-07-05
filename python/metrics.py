# Databricks notebook source
dbutils.widgets.text("pipeline_id", "")

# COMMAND ----------

df = spark.read.format("delta").load(f"dbfs:/pipelines/{dbutils.widgets.get('pipeline_id')}/system/events")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN aweaver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aweaver.iot_gold_country_agg ORDER BY total_devices DESC

# COMMAND ----------

# 140810
