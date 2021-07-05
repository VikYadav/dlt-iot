# Databricks notebook source
dbutils.widgets.text("directory", "dbfs:/tmp/iot/")
DIRECTORY = dbutils.widgets.get("directory")

# COMMAND ----------

#dbutils.fs.rm("dbfs:/tmp/iot/", recurse=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date

def write_good_data():

  df = spark.read.json("dbfs:/databricks-datasets/iot/").withColumn("timestamp", current_timestamp()).withColumn("date", current_date())
  df.write.mode("append").format("parquet").save(DIRECTORY)

# COMMAND ----------

write_good_data()

# COMMAND ----------

from pyspark.sql.functions import lit

def write_invalid_lcd():
  invalid_lcd = df.sample(False, 0.2, seed=0).withColumn("lcd", lit("blue"))
  invalid_lcd.write.mode("append").format("parquet").save(DIRECTORY)

# COMMAND ----------

#write_invalid_lcd()

# COMMAND ----------

def write_invalid_ip():
  invalid_ip = df.sample(False, 0.2, seed=0).withColumn("ip", lit("0.10.1.01"))
  invalid_ip.write.mode("append").format("parquet").save(DIRECTORY)

# COMMAND ----------

#write_invalid_ip()
