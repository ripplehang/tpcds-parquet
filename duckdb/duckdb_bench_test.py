import duckdb
import os
import boto3
import csv
import time
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Create DuckDB tables or views from Parquet files.')
parser.add_argument('--source', choices=['local', 's3'], required=True,
                    help='The source of the Parquet files (local or s3).')
parser.add_argument('--object-type', choices=['table', 'view'], required=True,
                    help='The type of database object to create (table or view).')
parser.add_argument('--data-size', choices=['1G', '10G', '100G'], required=True,
                    help='The size of the dataset to process (1G, 10G, 100G).')
parser.add_argument('--workdir',default='./', required=False,
                    help='Set the work dir.')
args = parser.parse_args()

# AWS credentials (use environment variables or IAM roles if possible)
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# S3 bucket and path to the TPC-DS dataset directory
s3_bucket_name = 'xizhu-test'
s3_tpcds_dir = f'tpcds/{args.data_size}/'

# Local directory containing SQL query files and Parquet files
query_directory = f'{args.workdir}/queries/'
local_parquet_directory = f'{args.workdir}/tpcds/{args.data_size}/'

# Database file and performance CSV file naming
database_file = f'{args.workdir}/database_{args.data_size}_{args.source}_{args.object_type}'
creation_performance_csv = f'{args.workdir}/creation_performance_data_{args.data_size}_{args.source}_{args.object_type}.csv'
query_performance_csv = f'{args.workdir}/query_performance_data_{args.data_size}_{args.source}_{args.object_type}.csv'

# Connect to DuckDB
con = duckdb.connect(database=database_file, read_only=False)

# Enable the HTTPFS extension to read from S3 if the source is 's3'
if args.source == 's3' and aws_access_key_id and aws_secret_access_key:
    con.execute("INSTALL 'httpfs'")
    con.execute("LOAD 'httpfs'")
    con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
    con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")

try:
    need_create_schema = False
    if need_create_schema:
        # Track the performance of table/view creation
        with open(creation_performance_csv, mode='w', newline='') as creation_file:
            creation_writer = csv.writer(creation_file)
            # Write header
            creation_writer.writerow(['Object Name', 'Object Type', 'Source', 'Creation Time (seconds)'])

            if args.source == 'local':
                # Process local Parquet files
                for item in os.listdir(local_parquet_directory):
                    item_path = os.path.join(local_parquet_directory, item)
                    if os.path.isdir(item_path):
                        table_name = item
                        start_time = time.time()
                        print(f"start to create {args.object_type.upper()} {table_name}")
                        con.execute(f"CREATE {args.object_type.upper()} {table_name} AS SELECT * FROM parquet_scan('{item_path}/*.parquet')")
                        end_time = time.time()
                        elapsed_time = end_time - start_time
                        print(f"{table_name} {args.object_type} created from local directory, cost {elapsed_time} s")
                        creation_writer.writerow([table_name, args.object_type, 'local', f'{elapsed_time:.2f}'])
                        creation_file.flush()
            elif args.source == 's3':
                # Process S3 Parquet files
                s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)
                response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_tpcds_dir, Delimiter='/')
                if 'CommonPrefixes' in response:
                    for prefix in response['CommonPrefixes']:
                        table_prefix = prefix['Prefix']
                        s3_file_pattern = f's3://{s3_bucket_name}/{table_prefix}*.parquet'
                        table_name = os.path.basename(table_prefix.strip('/'))
                        start_time = time.time()
                        con.execute(f"CREATE {args.object_type.upper()} {table_name} AS SELECT * FROM parquet_scan('{s3_file_pattern}')")
                        end_time = time.time()
                        elapsed_time = end_time - start_time
                        print(f"{table_name} {args.object_type} created from S3, cost {elapsed_time} s")
                        creation_writer.writerow([table_name, args.object_type, 's3', f'{elapsed_time:.2f}'])
                        creation_file.flush()
    # Execute SQL queries from files and measure time and row count
    with open(query_performance_csv, mode='w', newline='') as query_file:
        query_writer = csv.writer(query_file)
        # Write header
        query_writer.writerow(['Query File', 'Rows Returned', 'Execution Time (seconds)'])
        sql_files = sorted([f for f in os.listdir(query_directory) if f.endswith('.sql')], key=str.lower)

        for filename in sql_files:
            with open(os.path.join(query_directory, filename), 'r') as sql_file:
                query = sql_file.read()
                try:
                    start_time = time.time()
                    result = con.execute(query).fetchall()
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    row_count = len(result)
                    print(f'Results for {filename}: {row_count} rows, executed in {elapsed_time:.2f} seconds')
                    query_writer.writerow([filename, row_count, f'{elapsed_time:.2f}'])
                    query_file.flush()
                except Exception as e:
                    print(f'Error executing {filename}:', e)
                    query_writer.writerow([filename, 'Error', str(e)])

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the DuckDB connection
    con.close()