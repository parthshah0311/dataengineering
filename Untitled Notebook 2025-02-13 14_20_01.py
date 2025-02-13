# Databricks notebook source
# MAGIC %md
# MAGIC DDL

# COMMAND ----------

# %sql
# CREATE DATABASE IF NOT EXISTS forecast;

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("deltascope")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="deltascope",key="deltadbkey")
appid = dbutils.secrets.get(scope="deltascope",key="appid")
dictid = dbutils.secrets.get(scope="deltascope",key="dictid")

spark.conf.set("fs.azure.account.auth.type.deltastorage21g.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.deltastorage21g.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.deltastorage21g.dfs.core.windows.net",appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.deltastorage21g.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.deltastorage21g.dfs.core.windows.net", f"https://login.microsoftonline.com/{dictid}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@deltastorage21g.dfs.core.windows.net")

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://raw@deltastorage21g.dfs.core.windows.net/sales.csv")
df.show()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://processed@deltastorage21g.dfs.core.windows.net/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Day1

# COMMAND ----------

# df.write.mode("overwrite").format("delta").saveAsTable("forecast.sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Day2

# COMMAND ----------

from delta import DeltaTable

existing_delta_table = DeltaTable.forName(spark, "forecast.sales")

existing_delta_table.alias("existing").merge(df.alias("new"), "existing.OrderId = new.OrderId").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()