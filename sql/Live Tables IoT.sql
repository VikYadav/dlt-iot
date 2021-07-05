-- Databricks notebook source
-- DBTITLE 1,Ingest Raw Data to Bronze
CREATE LIVE TABLE iot_bronze(
  CONSTRAINT valid_date EXPECT (date <= current_date()), -- Firstly this should validate that this column contains a date, secondly that it is not a date in the future...
  CONSTRAINT valid_timestamp EXPECT (timestamp <= current_timestamp()) -- Firstly this should validate that this column contains a timestamp, secondly that it is not a timestamp in the future...
  ON VIOLATION FAIL UPDATE -- Fail the pipeline since we're Partitioning and Z-Ordering on Date/Time 
)
COMMENT "Bronze IoT Data, subject to some basic DQ checks"
PARTITIONED BY (date)
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols' = 'timestamp', 'data_quality' = 'bronze')
AS SELECT * FROM parquet.`dbfs:/tmp/iot/*.parquet`

-- COMMAND ----------

-- DBTITLE 1,Certify Data to Silver
CREATE LIVE TABLE iot_silver(
  CONSTRAINT valid_lcd_reading EXPECT (lcd IN ('red', 'yellow', 'green')), -- Indicator for the need for maintenance...
  CONSTRAINT valid_ip_address EXPECT (ip RLIKE '^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\\.(?!$)|$)){4}$'), -- So we can attempt remote diagnostics...
  CONSTRAINT valid_ip_latitude EXPECT (latitude RLIKE '^-?[0-9]{1,3}(?:\\.[0-9]{1,10})?$'), -- So our engineers can find the device...
  CONSTRAINT valid_ip_longitude EXPECT (longitude RLIKE '^-?[0-9]{1,3}(?:\\.[0-9]{1,10})?$') -- So our engineers can find the device...
)
COMMENT "Silver IoT Data, ready for downstream analytcs"
PARTITIONED BY (date)
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols' = 'timestamp', 'data_quality' = 'silver')
AS SELECT * FROM LIVE.iot_bronze

-- COMMAND ----------

-- DBTITLE 1,Aggregate to Gold
CREATE LIVE TABLE iot_gold_country_agg(
  CONSTRAINT valid_total EXPECT (total_devices > 0) -- An aggregation should only exist if there are 1 or more records for it
  ON VIOLATION FAIL UPDATE 
)
COMMENT "Gold IoT Data, aggregated by country"
TBLPROPERTIES ('data_quality' = 'gold')
AS SELECT cca3, COUNT(*) AS total_devices FROM LIVE.iot_silver GROUP BY cca3
