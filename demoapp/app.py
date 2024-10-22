from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import os


st.set_page_config(layout="wide")
# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )
 
user_info = get_user_info()

st.write(user_info)

with st.expander("Header"):
    headers = st.context.headers
    st.write(headers)


def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()




@st.cache_data(ttl=600)  # only re-query if it's been 600 seconds
def getData():

    return sqlQuery("select * from app_dev.default.people ")

data = getData()

data["Select"] = False

edited_df = st.data_editor(data,disabled=["id"])

filtered_df = edited_df[edited_df['Select']]

if filtered_df.empty:
    pass


else:
    st.write("Validate selected rows and update")

    st.dataframe(filtered_df)

    if update_button:
        st.write("Updating rows...")
        cfg = Config()
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                for index, row in filtered_df.iterrows():
                    try:
                        # Assuming 'id' is the primary key and should not be updated
                        columns = [col for col in row.index if col != "id" and col != "Select"]
                        updates = ", ".join([f"{col} = '{row[col]}'" for col in columns])
                        update_query = f"UPDATE app_dev.default.people SET {updates} WHERE id = {row['id']}"
                        cursor.execute(update_query)
                    except Exception as e:
                        st.error(f"An error occurred while updating row {index}: {e}")
                        st.stop()

        data = getData()
        st.write("Updated Data:")
        st.write(data)
        



with st.expander("Add rows to data"):
# Create a form to collect user information
    with st.form(key='user_form'):
        # Dynamically create input fields based on data columns
        inputs = {}
        for column in data.columns:
            # Assuming the data is composed of simple data types like text and numbers
            inputs[column] = st.text_input(label=column)

        # Add a submission button
        submit_button = st.form_submit_button(label='Submit')

    # Handle the form submission
    if submit_button:
        # Collect the inputs into a DataFrame
        user_input_data = pd.DataFrame([inputs])
        st.write('Submitted Data:')
        st.write(user_input_data)
            # Insert the user input data into Databricks
        cfg = Config() # Pull environment variables for auth
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                
                try:
                    # Assuming data columns match table columns
                    table_name = "app_dev.default.people"

                    cursor.execute(f"INSERT INTO {table_name} VALUES {tuple(user_input_data.iloc[0])}")

                    data = getData()
                    st.write(f"Data from {table_name}")
                    st.write(data)
                except Exception as e:
                    st.error(f"An error occurred while inserting data: {e}")
                    st.stop()



