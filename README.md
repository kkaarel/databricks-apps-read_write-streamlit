# Databricks Reference Table Management App

This Streamlit application allows users to interact with and manage reference tables stored in Databricks. Developed to streamline the process of querying, updating, and inserting data within reference tables, the app is designed for ease of use and flexibility.

## Features

- **User Info Display**: Automatically retrieves and displays user information such as username, email, and user ID based on HTTP headers.
- **Data Query**: Connects to a Databricks SQL warehouse to fetch data from a table (`app_dev.default.people`) and displays it in an editable format.
- **Row Selection and Update**: Allows users to select rows for updating, edit their content, and submit changes back to the Databricks table.
- **Data Insertion**: Provides a form to add new rows to the reference table. The form collects input for each column and inserts the data into the Databricks table.

## Usage

1. **Pre-requisites**: Ensure the `DATABRICKS_WAREHOUSE_ID` environment variable is set in your app.yaml.
2. **User Information**: Verify that you can see your user information in the "User info" section.
3. **Data Display**: View and interact with the data fetched from the Databricks table.
4. **Update Rows**: Select rows by checking the "Select" checkbox, edit their values, and click "Update Rows" to save the changes to Databricks.
5. **Add Rows**: Use the form under the "Add rows to data" section to insert new data into the reference table.

## Developer and Contributions

Developed by [Kaarel Korvemaa](https://www.linkedin.com/in/korvemaa/). For the source code and contributions, visit [GitHub Repository](https://github.com/kkaarel/databricks-apps-cicd-streamlit).

## Configuration

You don't really need to configure anything to the file. 

- You need to add the service principal that is created in the deployemnt to have access to the data source
- Has to have rights to update the table 



## Disclaimer

This application is a demo and provided as-is. Please validate the code and configuration settings according to your environment and security practices before deploying in a production environment.


Databricks official documentaion about apps: 

[What are data apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)

[Getting started with data apps](https://docs.databricks.com/en/dev-tools/databricks-apps/get-started.html)