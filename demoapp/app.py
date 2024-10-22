import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import requests

# Ensure environment variable is set correctly
st.set_page_config(layout="wide")



assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Ensure that secrets are correctly set up
#try:
#    DATABRICKS_WAREHOUSE_ID = st.secrets["DATABRICKS_WAREHOUSE_ID"]
#    st.write("DATABRICKS_WAREHOUSE_ID:", DATABRICKS_WAREHOUSE_ID)
#except FileNotFoundError:
#    st.error("No secrets found. Please make sure that the secrets.toml file is properly configured.")
#    st.stop()

headers = st.context.headers
with st.expander("Headers"):
    st.write(headers)
with st.expander("Environment variables"):
    st.write(dict(os.environ))

host = headers["DATABRICKS_HOST"]

st.write(f"header host {host}")

def sqlQuery(query: str) -> pd.DataFrame:
    #cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=headers["DATABRICKS_HOST"],
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.write("Server Hostname:", server_hostname)
st.write("Host Path:", os.getenv("DATABRICKS_HOST"))

def credential_provider():
  config = Config(
    host          = f"https://{host}",
    client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET"))
  return oauth_service_principal(config)

with sql.connect(server_hostname      = server_hostname,
                 http_path            = os.getenv("DATABRICKS_HTTP_PATH"),
                 credentials_provider = credential_provider) as connection:


@st.cache_data(ttl=600)  # only re-query if it's been 600 seconds
def getData():
    # Update query to use the new people table
    return sqlQuery("select * from app_dev.default.people limit 5000")
