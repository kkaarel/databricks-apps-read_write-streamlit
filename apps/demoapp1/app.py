from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import os


st.set_page_config(
    layout="wide",
    page_title="Write and update your reference tables in Databricks",
    page_icon="👋"
)
# Ensure environment variable is set correctly
#assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

#DATABRICKS_WAREHOUSE_ID = ""  #st.write(os.getenv('DATABRICKS_WAREHOUSE_ID'))

st.write(os.environ)

DATABRICKS_WAREHOUSE_ID = st.text_input(
    "Enter Databricks Warehouse ID:👋",
    value=os.getenv('DATABRICKS_WAREHOUSE_ID', ''),
    help="Please provide the Databricks Warehouse ID if it's not set as an environment variable."
)

if not DATABRICKS_WAREHOUSE_ID:
    st.error("DATABRICKS_WAREHOUSE_ID must be provided.")
    st.stop()



def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )
 
user_info = get_user_info()


with st.expander("User info"):
    st.write(user_info)


def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    try:
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        st.error(f"An error occurred while connecting to the database: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


if 'stage' not in st.session_state:
    st.session_state.stage = 0

def set_state(i):
    st.session_state.stage = i


#@st.cache_data(ttl=600)  # IF you want to query realtime and see the changes using this is not smart
def getData():

    return sqlQuery("select * from app_dev.default.people ")

data = getData()

data["Select"] = False


st.write("This Streamlit app integrates with Databricks to allow users to view, update, and insert rows in a reference table within a Databricks.")


edited_df = st.data_editor(data,disabled=["id"])

filtered_df = edited_df[edited_df['Select']]

if filtered_df.empty:
    pass


else:
    st.write("Validate selected rows and update")

    st.dataframe(filtered_df)
    update_button = st.button('Update Rows')

    if update_button:
        with st.spinner('Updating rows...'):
            cfg = Config()
            with sql.connect(
                server_hostname=cfg.host,
                http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
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
        
#if st.session_state.stage == 0:
#    st.button('Begin', on_click=set_state, args=[1])

set_state(1)
with st.expander("Add rows to data"):
    if st.session_state.stage >= 1:
        with st.form(key='user_form'):

            inputs = {}
            for column in data.columns:

                if column != "Select":
                    inputs[column] = st.text_input(label=column)


            submit_button = st.form_submit_button(label='Submit', on_click=set_state, args=[2])


        if submit_button:

            user_input_data = pd.DataFrame([inputs])
            st.write('Submitted Data:')
            with st.spinner('Updating rows...'):
            #st.write(user_input_data)
                if st.session_state.stage >= 2:
                    set_state(3)
                        
                    cfg = Config() 
                    with sql.connect(
                        server_hostname=cfg.host,
                        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
                        credentials_provider=lambda: cfg.authenticate
                    ) as connection:
                        with connection.cursor() as cursor:
                            
                            try:
                    
                                table_name = "app_dev.default.people"

                                cursor.execute(f"INSERT INTO {table_name} VALUES {tuple(user_input_data.iloc[0])}")

                                data = getData()
                                st.write(f"Data from {table_name}")
                                st.write(data)

                            except Exception as e:
                                st.error(f"An error occurred while inserting data: {e}")
                                st.stop()



