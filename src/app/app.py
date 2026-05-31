import os
import streamlit as st
from databricks import sql

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "default")

st.title("insurance_poc_databricks_demo App")
st.write(f"Connected to `{CATALOG}.{SCHEMA}`")

if st.button("List Tables"):
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: {"token": os.environ["DATABRICKS_TOKEN"]},
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
            rows = cursor.fetchall()
    st.dataframe(rows)
