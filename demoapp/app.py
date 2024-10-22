import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
st.set_page_config(layout="wide")



#assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Ensure that secrets are correctly set up
try:
    DATABRICKS_WAREHOUSE_ID = st.secrets["DATABRICKS_WAREHOUSE_ID"]
    st.write("DATABRICKS_WAREHOUSE_ID:", DATABRICKS_WAREHOUSE_ID)
except FileNotFoundError:
    st.error("No secrets found. Please make sure that the secrets.toml file is properly configured.")
    st.stop()


def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    DATABRICKS_WAREHOUSE_ID = st.secrets["DATABRICKS_WAREHOUSE_ID"]

    try:
        st.write(f"Connecting to Databricks at {cfg.host} with warehouse ID: {DATABRICKS_WAREHOUSE_ID}")
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            st.write("Connection established successfully.")
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"Error during SQL query execution: {e}")
        raise

@st.cache_data(ttl=600)  # only re-query if it's been 600 seconds
def getData():
    # Update query to use the new people table
    return sqlQuery("select * from app_dev.default.people limit 5000")

data = getData()

st.header("People Data Distribution")
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("Age vs. ID")
    st.scatter_chart(data=data, height=400, width=700, y="age", x="id")

with col2:
    st.subheader("Search Person by Name")
    name = st.text_input("Name")
    person = data[(data['name'].str.contains(name, case=False))]
    st.write(f"# **{person.iloc[0]['email'] if len(person) > 0 else 'No match found'}**")

st.dataframe(data=data, height=600, use_container_width=True)