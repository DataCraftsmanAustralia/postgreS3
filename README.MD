# PostgreS3

This program turns your Postgres Server into an S3 store. You can create buckets, store files using keys and retrieve in the same way.
It's gross but would totally be useful for someone.


# Environment setup
Copy the .env.template as .env

S3_ACCESS_KEY = your postgres username
S3_SECRET_KEY = your postgres password
POSTGRES_SERVER = "localhost:5432/postgres" replace with your servers details

# Install dependencies
pip install -r requirements.txt

# Usage

## Create client with username/password
client = PostgresStorageClient(
    POSTGRES_SERVER,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

## Create a bucket
client.make_bucket("test-bucket")

## Upload a file
client.fput_object(
    "test-bucket",
    "test-file.txt",
    "testfile.txt",
    content_type="text/plain"
)

## Download a file
client.fget_object(
    "test-bucket",
    "test-file.txt",
    "downloaded-file.txt"
)