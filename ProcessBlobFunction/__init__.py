import logging
import os
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import psycopg2
import json

from azure.functions import InputStream

# Initialize Azure Document Intelligence client
def get_document_analysis_client():
    endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("DOCUMENT_INTELLIGENCE_KEY")
    return DocumentAnalysisClient(endpoint, AzureKeyCredential(key))

# Initialize PostgreSQL connection
def get_postgresql_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

def main(myblob: InputStream):
    logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")

    # Analyze the document using Azure Document Intelligence
    client = get_document_analysis_client()
    poller = client.begin_analyze_document("prebuilt-invoice", myblob.read())
    result = poller.result()

    # Extract fields from the document
    extracted_data = {}
    for field_name, field in result.fields.items():
        extracted_data[field_name] = field.value

    # Write extracted data to PostgreSQL
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT INTO invoices (data) VALUES (%s)""",
            (json.dumps(extracted_data),)
        )
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to insert data into PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

    logging.info("Blob processing completed.")
