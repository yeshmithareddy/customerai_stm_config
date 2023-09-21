import boto3
import pandas as pd
import streamlit as st
from io import StringIO
import snowflake.connector

# Connection parameters for Snowflake and S3
connection_parameters1 = {
    "account": "anblicksorg_aws.us-east-1",
    "user": "CUSTOMERAI",
    "password": "CustomerAI@202308",
    "role": "CUSTOMERAI_ARL",
    "warehouse": "CUSTOMERAI_WH",
    "database": "CUSTOMERAI_DB",
    "schema": "MAIN"
}

connection_parameters2 = {
    "account": "anblicksorg_aws.us-east-1",
    "user": "CUSTOMERAI",
    "password": "CustomerAI@202308",
    "role": "CUSTOMERAI_ARL",
    "warehouse": "CUSTOMERAI_WH",
    "database": "CUSTOMERAI_DB",
    "schema": "CONFIG"
}

# Creating the low-level functional client for S3
client = boto3.client(
    's3',
    aws_access_key_id='AKIA23KQLPGLTNJ2RNFV',
    aws_secret_access_key='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',
    region_name='us-east-1'
)

# Streamlit app title
st.title("CSV File Selector and Snowflake Table Viewer")

# Create a layout with two columns
left_column, right_column = st.columns(2)

# Define the store_mapping function to save mappings to a JSON file
def store_mapping(mapping_key, mapping_value):
    mapping_data = {}
    if st.session_state.get('column_mappings'):
        mapping_data = st.session_state.column_mappings

    if mapping_key not in mapping_data:
        mapping_data[mapping_key] = []

    mapping_data[mapping_key].append(mapping_value)
    st.session_state.column_mappings = mapping_data

# Define the load_mappings function to retrieve existing mappings from the JSON file
def load_mappings(mapping_key):
    mapping_data = st.session_state.get('column_mappings', {})
    return mapping_data.get(mapping_key, [])

# Part 1: CSV File Selector
with left_column:
    st.header("CSV File Selector from S3")

    # Specify your S3 bucket name
    bucket_name = 'custai'
    # List objects (files) in the S3 bucket
    s3_objects = client.list_objects(Bucket=bucket_name)
    # Extract the list of file names from the S3 objects
    file_list = [obj['Key'] for obj in s3_objects.get('Contents', []) if obj['Key'].endswith('.csv')]
    # Dropdown menu for selecting a CSV file
    selected_file = st.selectbox("Select a CSV file from S3", file_list)
    # Display the selected file
    st.write(f"You selected: {selected_file}")

    if selected_file:
        # Read CSV file from S3
        csv_object = client.get_object(Bucket=bucket_name, Key=selected_file)
        csv_content = csv_object['Body'].read().decode('utf-8')

        # Create a Pandas DataFrame from CSV content
        df_csv = pd.read_csv(StringIO(csv_content))

        # Display columns from the DataFrame
        selected_csv_column = st.multiselect("Select a CSV Column", df_csv.columns.tolist())

# Part 2: Snowflake Table Viewer
with right_column:
    st.header("Snowflake Table Viewer")

    conn = snowflake.connector.connect(**connection_parameters1)

    # Fetch specific table names from Snowflake
    desired_tables = ["CAI_PRODUCT", "CAI_SURVEY", "CAI_LEAD_SEGMENTATION", "CAI_INVOICE", "CAI_CUSTOMER", "CAI_CHANNEL_ATTRIBUTION"]
    table_names = []
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        for row in cursor.fetchall():
            table_name = row[1]
            if table_name in desired_tables:
                table_names.append(table_name)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    selected_table = st.selectbox("Select a Snowflake Table", table_names)

    # Display table names
    st.write("Snowflake Column Names:")

    conn = snowflake.connector.connect(**connection_parameters1)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS IN {selected_table}")
        snowflake_column_names = [row[2] for row in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    selected_snowflake_column = st.multiselect("Select Snowflake Columns", snowflake_column_names)

#  Add a "Preview Data" button
if st.button("Preview Data", type="primary"):
    if selected_file:
        # Display a preview of the data
        st.write("Preview of Data:")
        st.write(df_csv.head())  # You can customize the number of rows to display

# Add a submit button to update mappings in the STM_CONFIG table
if st.button("Submit"):
    if selected_csv_column and selected_snowflake_column:
        mapping_key = (selected_file, selected_table)
        mapping_value = list(zip(selected_csv_column, selected_snowflake_column))

        # Clear existing mappings for the same CSV file and Snowflake table
        mapping_data = st.session_state.get('column_mappings', {})
        if mapping_key in mapping_data:
            del mapping_data[mapping_key]
            st.session_state.column_mappings = mapping_data

        store_mapping(mapping_key, mapping_value)
        st.success("Mapping saved successfully!")

        # Update the mappings in the STM_CONFIG table
        conn = snowflake.connector.connect(**connection_parameters2)
        try:
            cursor = conn.cursor()
            for csv_column, snowflake_column in mapping_value:
                cursor.execute(
                    f"""
                    UPDATE STM_CONFIG
                    SET TARGET_COLUMN = %s
                    WHERE SOURCE_SCHEMA = %s
                      AND SOURCE_TABLE = %s
                      AND SOURCE_COLUMN = %s
                      AND TARGET_SCHEMA = %s
                      AND TARGET_TABLE = %s;
                    """,
                    (snowflake_column, connection_parameters1["schema"], selected_table, csv_column, connection_parameters2["schema"], "STM_CONFIG")
                )
            conn.commit()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# Display mapped columns and "Preview Data" buttons
if selected_file and selected_table and selected_snowflake_column:
    mapping_key = (selected_file, selected_table)
    existing_mappings = load_mappings(mapping_key)
    if existing_mappings:
        st.header("Mappings")
        table_data = [{"Source Column": csv_column, "Target Column": snowflake_column} for mapping in existing_mappings for csv_column, snowflake_column in mapping]
        st.table(pd.DataFrame(table_data))
