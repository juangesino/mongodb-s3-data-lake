import sys
import os
import yaml
import gzip
from datetime import datetime, timedelta

import boto3
from bson.json_util import dumps as mongo_dumps
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get AWS credentials
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Get MongoDB credentials
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_NAME = os.getenv("MONGODB_NAME")


# Generator to iterate MongoDB results in batches
def iterate_by_chunks(cursor, chunksize=50000, start_from=0, query={}):
    """
    Function to iterate MongoDB results in batches
    """
    chunks = range(start_from, cursor.count_documents(query), int(chunksize))
    num_chunks = len(chunks)

    for i in range(1, num_chunks + 1):
        if i < num_chunks:
            yield cursor.find(query)[chunks[i - 1] : chunks[i]]
        else:
            yield cursor.find(query)[chunks[i - 1] : chunks.stop]


# Generator to iterate days between two dates
def date_range(start_date, end_date, step=1):
    """
    Iterates through a range of dates

    Parameters
    ----------
    start_date : str
        The starting day in YYYY-MM-DD format (e.g.: `2021-01-30`)

    end_date : str
        The end point of the sequence (not part of the generated sequence) (e.g.: `2021-02-05`)

    step : int
        Loop increment in number of days

    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    if end_date <= start_date:
        raise ValueError("End date should be after start date.")

    current_date = start_date

    while current_date < end_date:
        yield current_date
        current_date = min([current_date + timedelta(days=step), end_date])


# Function to store records as compressed JSON file
def generate_file(records, file_name):
    """
    Stores the given list as a gzip compressed JSON file
    """

    # Dump and encode records
    json_str = mongo_dumps(records)
    json_bytes = json_str.encode("utf-8")

    # Store file as compressed JSON
    with gzip.GzipFile(file_name, "w") as file_out:
        file_out.write(json_bytes)


# Function to upload files to AWS S3
def upload_file(file_path, bucket_path, bucket_name=AWS_S3_BUCKET, purge=True):
    """
    Uploads a file to AWS S3

    Parameters
    ----------
    file_path : str
        The path to the file to be uploaded (e.g.: `/temp/file_name.json`)

    bucket_path : str
        The path inside the S3 bucket (e.g.: `date/filename.json`)

    bucket_name : str
        The name of the AWS S3 bucket

    purge : boolean
        Determines if the file is deleted after upload

    """

    # Start an S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # Upload file to S3 bucket
    s3_client.upload_file(
        file_path,
        bucket_name,
        bucket_path,
    )

    if purge:

        # Delete the file
        os.remove(file_path)


# Get name of the config file to use from arguments
# e.g.: python main.py some_config_file_name
config_name = sys.argv[1]

# Read the YAML config file
with open(f"config/{config_name}.yaml", "r") as stream:
    config = yaml.safe_load(stream)

# Read parameters from config file
COLLECTION_NAME = config["collection_name"]
START_DATE = str(config.get("start_date"))
END_DATE = str(config.get("end_date"))
DAYS_INTERVAL = 1
MINUTES_INTERVAL = (
    config.get("hours_interval", 1) * 60
    if "hours_interval" in config.keys()
    else config.get("minutes_interval", 60)
)
BATCH_SIZE = config.get("batch_size", 5000)
INCREMENTAL = config.get("incremental", False)


# Generate a timestamp for idempotence
today_str = datetime.now().strftime("%Y%m%d%H%M%S%f")

# If the extraction is not incremental we extract all and upload
if not INCREMENTAL:

    # Connect to MongoDB
    mongo_client = MongoClient(MONGODB_URI)
    mongo_db = mongo_client[MONGODB_NAME]
    collection = mongo_db[COLLECTION_NAME]

    # Keep track of batch number
    batch_number = 0

    # Iterate the query in batches
    for cursor in iterate_by_chunks(collection, query={}, chunksize=BATCH_SIZE):

        # Get data from cursor
        raw_data = list(cursor)

        # Check if there are any records
        if len(raw_data) > 0:

            print(f"Batch #{batch_number} ({len(raw_data)})")

            # Use timestamp to generate a file name
            file_name = f"{COLLECTION_NAME}__{today_str}_batch{batch_number}.json.gz"

            # Generate compressed file
            generate_file(raw_data, file_name)

            # Upload file to AWS S3
            upload_file(file_name, f"{COLLECTION_NAME}/{file_name}", AWS_S3_BUCKET)

        # Next batch of records
        batch_number += 1

else:

    # Iterate days between start and end date
    for dt in date_range(
        str(START_DATE),
        str(END_DATE),
        step=DAYS_INTERVAL,
    ):
        date_str = dt.strftime("%Y-%m-%d")
        date_str_file = dt.strftime("%Y%m%d")
        day = datetime.strptime(date_str, "%Y-%m-%d")

        print(f"\n\nLoad from {date_str}\n---------------------\n")

        # Iterate day in minutes intervals
        for lower_limit in range(0, 1440, MINUTES_INTERVAL):
            upper_limit = min(lower_limit + MINUTES_INTERVAL, 1440)

            # Styling for task ID
            hr_lower_limit = int(lower_limit / 60)
            hr_upper_limit = int(upper_limit / 60)
            min_lower_limit = int(lower_limit % 60)
            min_upper_limit = int(upper_limit % 60)

            # Adding zero padding to task ID
            z_lower_limit = str(hr_lower_limit).zfill(2) + str(min_lower_limit).zfill(2)
            z_upper_limit = str(hr_upper_limit).zfill(2) + str(min_upper_limit).zfill(2)

            print(f"{z_lower_limit} - {z_upper_limit}")

            # Build interval dates
            initial_dt = datetime.combine(day, datetime.min.time()) + timedelta(
                minutes=lower_limit
            )
            final_dt = datetime.combine(day, datetime.min.time()) + timedelta(
                minutes=upper_limit
            )

            # Connect to MongoDB
            mongo_client = MongoClient(MONGODB_URI)
            mongo_db = mongo_client[MONGODB_NAME]
            collection = mongo_db[COLLECTION_NAME]

            # Build aggregation pipeline for MongoDB
            or_list = []

            # Check if there is a single field
            if "date_field" in config.keys():
                or_list.append(
                    {
                        config["date_field"]: {
                            "$gte": initial_dt,
                            "$lt": final_dt,
                        }
                    }
                )

            # Check if there are multiple date fields
            if "date_fields" in config.keys():
                for date_field in config["date_fields"]:
                    or_list.append(
                        {
                            date_field: {
                                "$gte": initial_dt,
                                "$lt": final_dt,
                            }
                        }
                    )

            # Get data from MongoDB
            query = {"$or": or_list}

            # Keep track of batch number
            batch_number = 0

            # Iterate the query in batches
            for cursor in iterate_by_chunks(
                collection, query=query, chunksize=BATCH_SIZE
            ):

                # Get data from cursor
                raw_data = list(cursor)

                # Check if there are any records
                if len(raw_data) > 0:

                    print(f"Batch #{batch_number} ({len(raw_data)})")

                    # Use timestamp to generate a file name
                    file_name = f"{COLLECTION_NAME}_{date_str_file}_{z_lower_limit}-{z_upper_limit}_{today_str}_batch{batch_number}.json.gz"

                    # Generate compressed file
                    generate_file(raw_data, file_name)

                    # Upload file to AWS S3
                    upload_file(
                        file_name, f"{COLLECTION_NAME}/{file_name}", AWS_S3_BUCKET
                    )

                # Next batch of records
                batch_number += 1
