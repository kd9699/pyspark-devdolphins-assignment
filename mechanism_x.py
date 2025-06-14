import pandas as pd
import boto3
import time
import os

# The name of the S3 bucket created for landing transaction files.
S3_BUCKET_NAME = "devdolphines-task-transaction-landing-krishna" 

# The path to the transactions.csv.
TRANSACTIONS_FILE_PATH = r"C:\Users\kndas\Downloads\transactions.csv"

# the AWS region your S3 bucket
AWS_REGION = "us-east-1"

# ------------------------------------

# Constants from the assignment
CHUNK_SIZE = 10000

def run_mechanism_x():
    """
    Reads the transactions CSV in chunks and uploads each chunk to S3
    every second to simulate a real-time stream.
    """
    print("Starting Mechanism X...")
    print(f"Reading from: {TRANSACTIONS_FILE_PATH}")
    print(f"Uploading to S3 bucket: {S3_BUCKET_NAME} in region {AWS_REGION}")
    
    # Initialize the S3 client
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        # Check if the bucket exists by making a HeadBucket call
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print("Successfully connected to S3 and verified bucket exists.")
    except Exception as e:
        print(f"Error connecting to S3 or finding bucket. Please check configuration. Error: {e}")
        return

    chunk_index = 0
    try:
        # Create an iterator to read the CSV in chunks
        csv_iterator = pd.read_csv(TRANSACTIONS_FILE_PATH, chunksize=CHUNK_SIZE)
        
        for chunk_df in csv_iterator:
            # Generate a unique filename for the chunk
            file_name = f"transactions_chunk_{int(time.time())}_{chunk_index}.csv"
            
            # Convert DataFrame chunk to CSV format in memory
            csv_buffer = chunk_df.to_csv(index=False)
            
            # Upload the CSV data to S3
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=csv_buffer)
            
            print(f"SUCCESS: Uploaded chunk {chunk_index} as {file_name}")
            
            chunk_index += 1
            # Wait for 1 second before processing the next chunk, as per the assignment
            time.sleep(1)
            
    except FileNotFoundError:
        print(f"FATAL ERROR: The file was not found at {TRANSACTIONS_FILE_PATH}")
        print("Please check the TRANSACTIONS_FILE_PATH variable in the script.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_mechanism_x()