# Databricks notebook source
pip install datatable

# COMMAND ----------

import numpy as np
import pandas as pd
import pyspark.pandas as ps
import datatable as dt 

# COMMAND ----------

path = "adl://adlsanalyticsprd001.azuredatalakestore.net/curated/surface/busesbi/speed/2022/12/01/busspeeds_20221201.csv"
df = spark.read.option("header", "true").option("delimiter", "\t").format("csv").load(path)#.createOrReplaceTempView("BusSpeedSample")

# COMMAND ----------

df.display()

# COMMAND ----------

select_df = df.select('longitude', 'latitude', 'time', 'date', 'speedkph', 'speedkphdelta', 'drivingdirection', 'filedate')

# COMMAND ----------

select_df.display()

# COMMAND ----------

filtered_df = select_df.filter("drivingdirection = 302")

# COMMAND ----------

df_1 = filtered_df.filter("filedate = 20221128")

# COMMAND ----------

df_1.display()

# COMMAND ----------

df_1.orderBy('time').show()

# COMMAND ----------

filtered_df.display()


# COMMAND ----------

filtered_df.orderBy("date")
filtered_df.orderBy("time")

# COMMAND ----------

df.orderBy("time","date").show(truncate=False)


# COMMAND ----------

filtered_df.display()

# COMMAND ----------

# MAGIC %scala
# MAGIC val configs = Map(
# MAGIC   "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
# MAGIC   "dfs.adls.oauth2.client.id" -> "1fc613c0-d927-4146-a36a-6b2d44bcfa76",
# MAGIC   "dfs.adls.oauth2.credential" -> "yyj8Q~Kb7EQdgFAs4KA8mFjQiWPTcwczs8iSJbkR",
# MAGIC   "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/1fbd65bf-5def-4eea-a692-a089c255346b")
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "adl://adlsanalyticsprd001.azuredatalakestore.net/curated/surface/busesbi/speed/2022/12/01/busspeeds_20221201.csv",
# MAGIC   mountPoint = "/mnt/bus_acc_break",
# MAGIC   extraConfigs = configs)
