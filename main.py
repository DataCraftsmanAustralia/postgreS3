from postgreS3 import PostgresStorageClient
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Now you can access the variables
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")

# Create client with username/password
client = PostgresStorageClient(
    POSTGRES_SERVER,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)


# Create a bucket
client.make_bucket("test-bucket")

# Upload a file
client.fput_object(
    "test-bucket",
    "test-file.txt",
    "testfile.txt",
    content_type="text/plain"
)

# Download a file
client.fget_object(
    "test-bucket",
    "test-file.txt",
    "downloaded-file.txt"
)