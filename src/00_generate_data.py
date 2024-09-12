# Databricks notebook source
# MAGIC %pip install fsspec s3fs

# COMMAND ----------

dbutils.library.restartPython()

# cleanup from previous runs
spark.sql("Drop table if exists corbin.online_tables_demo.rain_daily")
dbutils.fs.rm("/tmp/checkpoint/rain_daily/", True)

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F


# Grab rain data from AWS Open Data NOAA Data Source
schema = "ID string, DATE string, DATA_VALUE long"

rain_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(schema)
    .load(
        "s3://noaa-ghcn-pds/parquet/by_year/YEAR={200[0-9],201[0-9],202[0-4]}/ELEMENT=PRCP/"
    )
    # Rename cols to lowercase
    .select(F.col("ID").alias("id"), F.col("DATE").alias("date"), F.col("DATA_VALUE").alias("data_value"))
    # Date is in the format "yyyyMMdd" and needs to be converted
    .withColumn("date", F.to_date("date", "yyyyMMdd"))
)

rain_df.writeStream.trigger(availableNow=True).option(
    "checkpointLocation", "/tmp/checkpoint/rain_daily/"
).toTable("corbin.online_tables_demo.rain_daily")

# COMMAND ----------

# Grab station data from AWS Open Data NOAA Data Source
stations_df = pd.read_fwf("s3://noaa-ghcn-pds/ghcnd-stations.txt", header=None, names=["id", "latitude", "longitude", "elevation", "name","unk", "gsn_flag", "wmo_id"], index_col=False).drop(["unk", "gsn_flag", "wmo_id"], axis=1)

stations_df = spark.createDataFrame(stations_df)

stations_df.write.mode("overwrite").saveAsTable("weather_stations")

# COMMAND ----------

# Cities data comes from https://simplemaps.com/resources/free-country-cities
cities_df = spark.read.table("corbin.online_tables_demo.cities")

city_closest_station_df = cities_df.join(stations_df.withColumnRenamed("id", "station_id"), (cities_df.lat >= stations_df.latitude - 0.5) & (cities_df.lat <= stations_df.latitude + 0.5) & (cities_df.lng >= stations_df.longitude - 0.5) & (cities_df.lng <= stations_df.longitude + 0.5), "inner")

city_closest_station_df.createOrReplaceTempView("city_closest_station")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE FUNCTION corbin.online_tables_demo.distance_udf(
# MAGIC   lat1 DOUBLE,
# MAGIC   lon1 DOUBLE,
# MAGIC   lat2 DOUBLE,
# MAGIC   lon2 DOUBLE
# MAGIC ) RETURNS DOUBLE LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate hearth distance from latitude and longitude' AS $$
# MAGIC   import numpy as np
# MAGIC   dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
# MAGIC   a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
# MAGIC   return 2 * 6371 * np.arcsin(np.sqrt(a))
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE corbin.online_tables_demo.cities_silver AS
# MAGIC with dist_tbl as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     corbin.online_tables_demo.distance_udf(lat, lng, latitude, longitude) as hearth_dist
# MAGIC   from
# MAGIC     city_closest_station
# MAGIC )
# MAGIC select
# MAGIC   city_ascii,
# MAGIC   lat,
# MAGIC   lng,
# MAGIC   country,
# MAGIC   iso2,
# MAGIC   iso3,
# MAGIC   capital,
# MAGIC   population,
# MAGIC   id,
# MAGIC   station_id as closest_station_id
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER(
# MAGIC         PARTITION BY id
# MAGIC         ORDER BY
# MAGIC           hearth_dist asc
# MAGIC       ) as rn
# MAGIC     from
# MAGIC       dist_tbl
# MAGIC   )
# MAGIC where
# MAGIC   rn = 1
