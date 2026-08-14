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
    # Clean host to ensure no protocol prefix exists
    host = (cfg.host or "").replace("https://", "").replace("http://", "").rstrip("/")
    
    # Clean and format warehouse HTTP path
    warehouse = DATABRICKS_WAREHOUSE_ID.strip()
    if warehouse.startswith("/sql/1.0/warehouses/"):
        http_path = warehouse
    else:
        http_path = f"/sql/1.0/warehouses/{warehouse}"

    # Authenticate as the App's Service Principal
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate
    )


def sql_query(query: str, suppress_error: bool = False) -> pd.DataFrame:
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        if not suppress_error:
            err_msg = str(e)
            if "INSUFFICIENT_PERMISSIONS" in err_msg or "42501" in err_msg:
                st.error(f"🔒 **Unity Catalog Access Denied**: {err_msg}")
            else:
                st.error(f"An error occurred while executing query: {e}")
        return pd.DataFrame()


def get_catalogs():
    df = sql_query("SHOW CATALOGS", suppress_error=True)
    if not df.empty:
        return df.iloc[:, 0].dropna().tolist()
    return []


def get_schemas(catalog: str):
    df = sql_query(f"SHOW SCHEMAS IN `{catalog}`", suppress_error=True)
    if not df.empty:
        return df.iloc[:, 0].dropna().tolist()
    return []


def get_tables(catalog: str, schema: str):
    df = sql_query(f"SHOW TABLES IN `{catalog}`.`{schema}`", suppress_error=True)
    if not df.empty:
        for col in df.columns:
            if "table" in col.lower():
                return df[col].dropna().tolist()
        return df.iloc[:, 1].dropna().tolist() if df.shape[1] > 1 else df.iloc[:, 0].dropna().tolist()
    return []


col1, col2, col3 = st.columns(3)

catalogs = get_catalogs()

if not catalogs:
    st.warning("No catalogs accessible or error connecting to Databricks SQL Warehouse.")
    st.stop()

with col1:
    selected_catalog = st.selectbox("Select Catalog", options=catalogs)

schemas = get_schemas(selected_catalog) if selected_catalog else []

with col2:
    selected_schema = st.selectbox("Select Schema", options=schemas) if schemas else None

tables = get_tables(selected_catalog, selected_schema) if (selected_catalog and selected_schema) else []

with col3:
    selected_table = st.selectbox("Select Table", options=tables) if tables else None

if not (selected_catalog and selected_schema and selected_table):
    st.info("Please select a Catalog, Schema, and Table to load data.")
    st.stop()

full_table = f"`{selected_catalog}`.`{selected_schema}`.`{selected_table}`"


def get_data():
    return sql_query(f"SELECT * FROM {full_table}")


data = get_data()

st.write("This Streamlit app integrates with Databricks to allow logged-in users to view, update, and insert rows in reference tables.")

if data.empty:
    st.info(f"💡 **Permission Required for {full_table}**")
    st.markdown(f"""
    To grant access to this table in Databricks Unity Catalog, run the following SQL commands in your Databricks SQL Editor or Notebook:

    ```sql
    GRANT USE CATALOG ON CATALOG `{selected_catalog}` TO `adminsdbw`;
    GRANT USE SCHEMA ON SCHEMA `{selected_catalog}`.`{selected_schema}` TO `adminsdbw`;
    GRANT SELECT, UPDATE, INSERT ON TABLE {full_table} TO `adminsdbw`;
    ```
    """)
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
