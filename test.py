import os
from minio import Minio
import io
from minio.error import S3Error

def upload_test_file():
    # Initialize Minio client with credentials
    client = Minio(
        "minio.lab.sspcloud.fr",
        access_key=os.getenv("MINIO_ACCESS_KEY_ID"),
        secret_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        session_token=os.getenv("MINIO_SESSION_TOKEN"),
        secure=True
    )
    
    # Bucket name where the file will be stored
    bucket_name = "alimane"

    # Ensure that the bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # File details
    file_name = "test_file.txt"
    file_content = "This is a test file."
    
    # Upload the file
    client.put_object(
        bucket_name,
        file_name,
        data=io.BytesIO(file_content.encode('utf-8')),
        length=len(file_content)
    )
    print(f"Uploaded {file_name} to bucket {bucket_name}")

if __name__ == "__main__":
    upload_test_file()
