import os
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
import streamlit as st
import pandas as pd
import requests

# Ensure environment variable is set correctly
st.set_page_config(layout="wide")



#assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

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

app_name = os.getenv("DATABRICKS_APP_NAME", "Not Found")
client_id = os.getenv("DATABRICKS_CLIENT_ID", "Not Found")
host = os.getenv("DATABRICKS_HOST", "Not Found")
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "Not Found")


# Display the extracted environment variables
st.write("App Name:", app_name)
st.write("Client ID:", client_id)
st.write("Host:", host)
st.write("Warehouse ID:", warehouse_id)




def credential_provider():
  config = Config(
    host          = f"https://{host}",
    client_id     = os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET"))
  return oauth_service_principal(config)

with sql.connect(server_hostname      = host,
                 http_path            = f"/sql/1.0/warehouses/{warehouse_id}",
                 credentials_provider = credential_provider) as connection:
    st.write("Successfully connected to Databricks SQL Warehouse")

@st.cache_data(ttl=600)  # only re-query if it's been 600 seconds
def getData():
    # Update query to use the new people table
    return sqlQuery("select * from app_dev.default.people limit 5000")
