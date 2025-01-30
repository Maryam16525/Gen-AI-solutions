# app.py
import os
import logging
import pyodbc
import streamlit as st
from dotenv import load_dotenv
from openai import AzureOpenAI
from textblob import TextBlob

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Access environment variables
api_key = os.getenv("OPENAI_API_KEY")
azure_endpoint = os.getenv("AZURE_ENDPOINT")
api_version = os.getenv("API_VERSION")

# Connection string for SQL Server - use environment variables for sensitive info
connection_string = os.getenv("SQL_CONNECTION_STRING")

# Establish a connection to SQL Server
def read_sql_query(sql):
    try:
        connection = pyodbc.connect(connection_string)
        logger.info("Connection successful")
        
        # Execute the query
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        connection.close()
        return rows
    
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
        return []

# Initialize Azure OpenAI client
client = AzureOpenAI(
    api_key=api_key,
    api_version=api_version,
    azure_endpoint=azure_endpoint
)

# Function to generate SQL code from a prompt using GPT model
def generate_code(prompt, model="gpt-4o"):
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

# Streamlit App
def run_streamlit_app():
    st.set_page_config(page_title="English Questions to SQL Query for gnossql Database")
    st.header("GPT-4o App To Retrieve SQL Data")
    
    # User input for the question
    question = st.text_input("Input: ", key="input")
    submit = st.button("Ask the question")

    if submit:
        prompt = """
        You are an expert in converting English questions to SQL query.
        The SQL database has a table named '[div].[app_events_nps]' with various columns.
        For SQL Server, use TOP to limit the number of rows.
        The SQL code should not have ``` in the beginning or end and should not include the word 'sql'.
        """
        prompt_with_question = prompt + f"\n\nQuestion: {question}"
        
        # Generate SQL code using GPT
        response = generate_code(prompt_with_question, model="gpt-4o")
        
        # Display the generated SQL code
        st.subheader("Generated SQL Code:")
        st.write(response)
        
        # Execute the SQL query and display results
        data = read_sql_query(response)
        st.subheader("The Response is:")
        for row in data:
            st.write(row)

if __name__ == "__main__":
    run_streamlit_app()
