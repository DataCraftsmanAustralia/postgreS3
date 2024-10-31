# PostgreS3

This program turns your Postgres Server into an S3 store. You can create buckets, store files using keys and retrieve in the same way.
The files are stored in LARGE OBJECT data type in postgres which allows you to store up to 4TB per file.
I have tested an 8GB file and it worked fine. It took 15mins to upload over network and 4 minutes to download. Could probably be improved with threading and file splitting or something.
I did notice that RAM went up 1:1 with the file size being uploaded. Not sure if it will limit the program or just write cache to disk.

It's a gross idea (and probably a gross implementation) but would be useful for someone.

# Use Cases
1. If you have a free postgres server but no free object storage for web hosting or something, you could use this on the postgres server.
2. Storing Data Science Models for archiving.
3. Open to ideas.

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