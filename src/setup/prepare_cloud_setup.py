"""

Script to prepare the cloud setup for the project. We mimick a standard "raw data in bucket" setup,
by creating a bucket (if does not exist) and uploading our tables in parquet format to it.

This script is intended to be run once, before the rest of the project is executed. None of this
code is particularly interesting, but makes the repository self-contained.

"""


from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import pyarrow.csv as pv
import pyarrow.parquet as pq
from os.path import join
import json
import pyarrow as pa


def csv_to_parquet(
    data_folder: str,
    csv_file_name: str
) -> str:
    _table = pv.read_csv(join(data_folder, csv_file_name))
    parquet_path = join(data_folder, csv_file_name.replace('.csv', '.parquet'))
    pq.write_table(_table, parquet_path)
    
    return parquet_path


def json_to_parquet(
    data_folder: str,
    json_file_name: str
) -> str:
    with open(join(data_folder, json_file_name), 'r') as f:
        rows_as_dict = json.load(f) 
        
    _table = pa.Table.from_pylist(rows_as_dict)
    parquet_path = join(data_folder, json_file_name.replace('.json', '.parquet'))
    pq.write_table(_table, parquet_path)
    
    return parquet_path


def create_bucket_if_not_exists(s3_client, s3_bucket_raw_data):
    try:
        s3_client.head_bucket(Bucket=s3_bucket_raw_data)
    except ClientError as e:
        # If a 404 error is thrown, then the bucket does not exist
        if e.response['Error']['Code'] == "404":
            s3_client.create_bucket(Bucket=s3_bucket_raw_data)
            return False
    
    return True # bucket already exists


def main(
    s3_bucket_raw_data: str,
    local_data_folder: str,
):
    print(f"Starting cloud setup preparation at {datetime.now()}")
    # initialize the S3 client
    # NOTE: we assume AWS credentials are set up in the environment
    s3_client = boto3.client('s3')  
    # keep track of the parquet files to upload
    parquet_files = []
    # get csv files to parquet
    for f in ['acquirer_countries.csv', 'payments.csv', 'merchant_category_codes.csv']:
        p_file = csv_to_parquet(local_data_folder, f)
        parquet_files.append(p_file)
    # get the JSON files to parquet
    for f in ['fees.json', 'merchant_data.json']:
        j_file = json_to_parquet(local_data_folder, f)
        parquet_files.append(j_file)
        
    assert len(parquet_files) == 5, "There should be 5 parquet files to upload"
    # create the S3 bucket if it does not exist
    does_bucket_exist = create_bucket_if_not_exists(s3_client, s3_bucket_raw_data)
    if not does_bucket_exist:
        print(f"Created a new S3 bucket {s3_bucket_raw_data}")
    # upload the parquet files to S3
    for p_file in parquet_files:
        print(f"Uploading {p_file} to S3 bucket {s3_bucket_raw_data}")
        s3_client.upload_file(p_file, s3_bucket_raw_data, p_file.split('/')[-1])
    
    print(f"Upload done at {datetime.now()}. See you, space cowboy!")
    return



if __name__ == "__main__":
    from dotenv import load_dotenv
    from os.path import dirname, abspath, join
    from os import environ
    d = dirname(dirname(abspath(__file__)))
    # Load environment variables from .env file in the src directory    
    load_dotenv(join(d, '.env'))
    assert 'S3_BUCKET_RAW_DATA' in environ, "S3_BUCKET_RAW_DATA environment variable is not set"
    
    main(
        s3_bucket_raw_data=environ['S3_BUCKET_RAW_DATA'],
        local_data_folder=join(dirname(abspath(__file__)), 'data')
    )