-- Databricks notebook source
create connection if not exists usgs_conn_1
type HTTP
options (
  host 'https://earthquake.usgs.gov',
  port '443',
  base_path '/earthquakes/feed/v1.0/',
  bearer_token 'na'
);

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text('catalog_name','youtube_earthquake_dev')
-- MAGIC catalog_name=dbutils.widgets.get('catalog_name')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC conn = w.connections.get("usgs_conn_1")
-- MAGIC
-- MAGIC base_url = f"{conn.options['host']}{conn.options['base_path']}"
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import requests
-- MAGIC import json
-- MAGIC import datetime
-- MAGIC
-- MAGIC url = f"{base_url}summary/all_day.geojson"
-- MAGIC try:
-- MAGIC     response = requests.get(url)
-- MAGIC     if(response.status_code == 200):
-- MAGIC         print("Success")
-- MAGIC         today=str(datetime.datetime.now().strftime("%Y-%m-%d"))
-- MAGIC         payload = response.json()
-- MAGIC         dbutils.fs.put(f"/Volumes/{catalog_name}/bronze/earthquake/date={today}.json", json.dumps(payload), True)
-- MAGIC     else:
-- MAGIC         raise Exception("Error in pulling data from USGS")
-- MAGIC except Exception as e:
-- MAGIC     print(e)
-- MAGIC
