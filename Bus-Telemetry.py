# Databricks notebook source
import numpy as np
import pandas as pd
import pyspark.pandas as ps
import datatable as dt
from pyspark.sql.functions import col

# COMMAND ----------

path = "adl://adlsanalyticsprd001.azuredatalakestore.net/curated/surface/busesbi/speed/2022/12/01/busspeeds_20221201.csv"
df = (
    spark.read.option("header", "true")
    .option("delimiter", "\t")
    .format("csv")
    .load(path)
)  # .createOrReplaceTempView("BusSpeedSample") #read in data

# COMMAND ----------

 df.selectExpr("cast(speedkph as double) speedkph")

# COMMAND ----------

df.display()

# COMMAND ----------

select_df = df.select(
    "longitude",
    "latitude",
    "time",
    "date",
    "speedkph",
    "speedkphdelta",
    "drivingdirection",
    "filedate",
    "registrationnumber"
)
#select these columns 

# COMMAND ----------

select_df.display()

# COMMAND ----------


select_df.select("registrationnumber").distinct().show() # shows the distinct values

# COMMAND ----------

df_1 = select_df.filter(select_df.registrationnumber == 'LG21JBE').display() #only this registration number shows a single bus

# COMMAND ----------

X = []
for j in range(len(select_df.speedkphdelta)):
    if df_1.speedkphdeltalen(range(A)) != 0 and df_1.speedkphdeltalen(range(A +1)) != 0:
        X.append(A[j])
    else:
        print('lol')
print(X)

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
