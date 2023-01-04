# Databricks notebook source
pip install datatable

# COMMAND ----------

import numpy as np
import pandas as pd
import pyspark.pandas as ps
import datatable as dt 

# COMMAND ----------


from datatable import (dt, f, by, ifelse, update, sort,
                       count, min, max, mean, sum, rowsum)

# COMMAND ----------

df = dt.fread('/dbfs/FileStore/sedona/part_00000_tid_4841385668007278789_e1fd6717_0ce4_44d3_ac2e_983397c4b9ff_27255_1_c000.csv')

# COMMAND ----------

df

# COMMAND ----------

columns_to_select = ['longitude', 'latitude', 'time', 'date', 'speedkph', 'speedkphdelta', 'drivingdirection']
bus_route_302 = df[:, columns_to_select].head(100)

# COMMAND ----------

bus_route_302 = df[:, count(f.time), by('longitude', 'latitude', 'time', 'date', 'speedkph', 'speedkphdelta', 'drivingdirection')]

# COMMAND ----------

bus_route_302 = df[f.drivingdirection == 302, :]


# COMMAND ----------


bus_route_302.sort('time')   

# COMMAND ----------

bus_route_302[f.speedkph > 10, :]

# COMMAND ----------

# MAGIC %scala
# MAGIC val configs = Map(
# MAGIC   "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
# MAGIC   "dfs.adls.oauth2.client.id" -> "1fc613c0-d927-4146-a36a-6b2d44bcfa76",
# MAGIC   "dfs.adls.oauth2.credential" -> "yyj8Q~Kb7EQdgFAs4KA8mFjQiWPTcwczs8iSJbkR",
# MAGIC   "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/1fbd65bf-5def-4eea-a692-a089c255346b")
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "adl://kpadls.azuredatalakestore.net/",
# MAGIC   mountPoint = "",
# MAGIC   extraConfigs = configs)

# COMMAND ----------

# MAGIC %scala
# MAGIC ls /mnt/kp-adls-testing
