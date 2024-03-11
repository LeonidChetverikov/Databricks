# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV to Delta").getOrCreate()

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/template.csv", header=True, inferSchema=True, schema="id INT,firstname STRING,lastname STRING,email STRING,email2 STRING,profession STRING")
df.show()
df.printSchema()

# COMMAND ----------

clientID = "<ClientID>"
clientSecret = "<ClientSecret>"
storageAccount = "epamwebinar1"

spark.conf.set("fs.azure.account.auth.type."+storageAccount+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+storageAccount+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+storageAccount+".dfs.core.windows.net", clientID)
spark.conf.set("fs.azure.account.oauth2.client.secret."+storageAccount+".dfs.core.windows.net", clientSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storageAccount+".dfs.core.windows.net", "https://login.microsoftonline.com/984f3b25-1363-40f2-bf57-014b6561b8ac/oauth2/token")



df.write.format("delta").save("abfss://data@"+storageAccount+".dfs.core.windows.net/testdata")

# COMMAND ----------

testdata = "abfss://data@"+storageAccount+".dfs.core.windows.net/testdata"
print(testdata)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS testdata 
# MAGIC USING parquet
# MAGIC LOCATION 'abfss://data@epamwebinar1.dfs.core.windows.net/testdata';
# MAGIC
# MAGIC ALTER TABLE testdata SET TBLPROPERTIES(delta.timeUntilArchived = '30 days') ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE testdata;
