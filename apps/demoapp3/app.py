from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import os


st.set_page_config(
    layout="wide",
    page_title="Write and update reference tables in Databricks",
    page_icon="👋"
)

DATABRICKS_WAREHOUSE_ID = st.text_input(
    "Enter Databricks Warehouse ID:",
    value=os.getenv('DATABRICKS_WAREHOUSE_ID', ''),
    help="Databricks SQL Warehouse ID"
)

if not DATABRICKS_WAREHOUSE_ID:
    st.error("DATABRICKS_WAREHOUSE_ID must be provided.")
    st.stop()


def get_user_info():
    headers = getattr(st, "context", None) and getattr(st.context, "headers", {}) or {}
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )

user_info = get_user_info()

with st.expander("User info"):
    st.write(user_info)


def get_connection():
    cfg = Config()
    headers = getattr(st, "context", None) and getattr(st.context, "headers", {}) or {}
    user_token = headers.get("X-Forwarded-Access-Token") or headers.get("Authorization", "").replace("Bearer ", "")

    if user_token:
        return sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
            access_token=user_token
        )
    else:
        return sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
            credentials_provider=lambda: cfg.authenticate
        )


def sql_query(query: str) -> pd.DataFrame:
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"An error occurred while executing query: {e}")
        return pd.DataFrame()


catalog_name = os.getenv("CATALOG_NAME", "")
schema_name = os.getenv("SCHEMA_NAME", "")
table_name = os.getenv("TABLE_NAME", "")

if not (catalog_name and schema_name and table_name):
    col1, col2, col3 = st.columns(3)
    with col1:
        catalog_name = st.text_input("Catalog", value=catalog_name, placeholder="catalog_name")
    with col2:
        schema_name = st.text_input("Schema", value=schema_name, placeholder="schema_name")
    with col3:
        table_name = st.text_input("Table", value=table_name, placeholder="table_name")

if not (catalog_name and schema_name and table_name):
    st.info("Please specify Catalog, Schema, and Table names to load data.")
    st.stop()

full_table = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"


def get_data():
    return sql_query(f"SELECT * FROM {full_table}")


data = get_data()

st.write("This Streamlit app integrates with Databricks to allow logged-in users to view, update, and insert rows in reference tables.")

if data.empty:
    st.info("No data returned. Ensure that the logged-in user has required Unity Catalog permissions (`USE CATALOG`, `USE SCHEMA`, `SELECT`) on the table.")
    st.stop()

data["Select"] = False

edited_df = st.data_editor(data, disabled=["id"] if "id" in data.columns else [])

if "Select" in edited_df.columns:
    filtered_df = edited_df[edited_df['Select']]
else:
    filtered_df = pd.DataFrame()

if not filtered_df.empty:
    st.write("Validate selected rows and update")
    st.dataframe(filtered_df)
    update_button = st.button('Update Rows')

    if update_button:
        with st.spinner('Updating rows...'):
            try:
                with get_connection() as connection:
                    with connection.cursor() as cursor:
                        for index, row in filtered_df.iterrows():
                            columns = [col for col in row.index if col != "id" and col != "Select"]
                            updates = ", ".join([f"`{col}` = '{row[col]}'" for col in columns])
                            update_query = f"UPDATE {full_table} SET {updates} WHERE id = {row['id']}"
                            cursor.execute(update_query)
                st.success("Rows updated successfully!")
            except Exception as e:
                st.error(f"An error occurred while updating: {e}")
                st.stop()

            data = get_data()
            st.write("Updated Data:")
            st.write(data)
