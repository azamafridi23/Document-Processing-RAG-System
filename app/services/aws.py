import boto3
import botocore
import os
import traceback
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

class AWS:
    """ Class used for AWS services"""

    def __init__(self) -> None:
        self.session = boto3.Session(profile_name='AE-v1')

    def download_file_from_s3(self, bucket_name, s3_file_key, local_file_path):
        """Downloads a file from S3 with the given bucket name, file key, and local file path."""
        try:
            print(
                f"Downloading file from s3: {s3_file_key} to {local_file_path}, from {bucket_name}")
            s3 = self.session.client('s3')
            s3.download_file(bucket_name, s3_file_key, local_file_path)
            print(
                f"Downloaded file from S3: {s3_file_key} to {local_file_path}")
            return True
        except Exception as e:
            print(f"Error occurred while downloading file from S3: {str(e)}")
            traceback.print_exc()
            return False

    def delete_s3_directory(self, bucket_name, prefix):
        """deletes s3 directory"""
        try:
            s3 = self.session.resource('s3')
            bucket = s3.Bucket(bucket_name)
            bucket.objects.filter(Prefix=prefix).delete()
            print(f"Deleted all objects in directory: {prefix}")
        except Exception as e:
            print(
                f"Error occurred while deleting objects in {prefix}: {str(e)}")
            traceback.print_exc()

    def upload_file_to_s3(self, local_file_path, bucket_name, s3_file_key):
        """uploads file to s3"""
        try:
            s3 = self.session.client('s3')
            s3.upload_file(local_file_path, bucket_name, s3_file_key, ExtraArgs={
                          'ContentDisposition': 'inline', 'ContentType': 'image/png'})
            print(f"Uploaded file to S3: {s3_file_key}")
        except Exception as e:
            print(f"Error occurred while uploading file to S3: {str(e)}")
            traceback.print_exc()

    def upload_directory_to_s3(self, local_directory_path, bucket_name, s3_directory_key):
        """Uploads a directory to S3 with the given bucket name and directory key using S3's Multipart Upload capabilities."""
        try:
            s3 = self.session.client('s3')
            for root, dirs, files in os.walk(local_directory_path):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(
                        local_path, local_directory_path)
                    s3_path = os.path.join(
                        s3_directory_key, relative_path).replace("\\", "/")
                    s3.upload_file(local_path, bucket_name, s3_path, ExtraArgs={
                                  'ContentDisposition': 'inline', 'ContentType': 'image/png'})
            print(f"Uploaded directory to S3: {s3_directory_key}")
        except Exception as e:
            print(f"Error occurred while uploading directory to S3: {str(e)}")
            traceback.print_exc()

    def delete_file_from_s3(self, bucket_name, s3_file_key):
        """Deletes a file from S3 with the given bucket name and file key."""
        try:
            s3 = self.session.resource('s3')
            obj = s3.Object(bucket_name, s3_file_key)
            obj.delete()
            print(f"Deleted file from S3: {s3_file_key}")
        except Exception as e:
            print(f"Error occurred while deleting file from S3: {str(e)}")
            traceback.print_exc()

    def list_objects_in_directory(self, bucket_name, directory_name):
        """Lists all the objects in an S3 directory."""
        s3 = self.session.client('s3')
        paginator = s3.get_paginator('list_objects_v2')

        # Ensure the directory name ends with a '/'
        if not directory_name.endswith('/') and directory_name != '':
            directory_name += '/'

        operation_parameters = {
            'Bucket': bucket_name,
            'Prefix': directory_name
        }

        page_iterator = paginator.paginate(**operation_parameters)

        list_of_file = []

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    list_of_file.append(obj['Key'])

        return list_of_file

    def list_directories(self, bucket_name):
        """Lists all the directories in an S3 bucket."""
        s3 = self.session.client('s3')
        paginator = s3.get_paginator('list_objects_v2')

        # This is the key part: setting the Delimiter to '/'
        operation_parameters = {'Bucket': bucket_name, 'Delimiter': '/'}

        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    print(prefix['Prefix'])

    def download_all_from_bucket(self, bucket_name, local_directory='./'):
        """Downloads all the objects in an S3 bucket to a local directory."""
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(bucket_name)

        for obj in bucket.objects.all():
            try:
                # Construct the full local path
                local_file_path = os.path.join(local_directory, obj.key)

                # Create local path directories
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # Download file
                bucket.download_file(obj.key, local_file_path)
                print(f"Downloaded {obj.key} to {local_file_path}")

            except Exception as e:
                print(f"Error downloading {obj.key}: {e}")

    def generate_object_url(self, bucket_name, s3_file_key):
        """Generates a URL-safe public URL for an object in S3."""
        try:
            # URL-encode the key to handle spaces and other special characters
            encoded_key = quote(s3_file_key)
            url = f"https://{bucket_name}.s3.amazonaws.com/{encoded_key}"
            return url
        except Exception as e:
            print(f"Error generating S3 object URL: {e}")
            return None

    def extract_s3_key_from_url(self, s3_url: str) -> tuple[str, str]:
        """
        Extracts bucket name and S3 key from a full S3 URL.
        
        Args:
            s3_url: Full S3 URL like "https://green-gro.s3.amazonaws.com/images/filename.png"
        
        Returns:
            tuple: (bucket_name, s3_key) or (None, None) if parsing fails
        """
        try:
            from urllib.parse import urlparse, unquote
            
            parsed_url = urlparse(s3_url)
            
            # Extract bucket name from hostname (green-gro.s3.amazonaws.com -> green-gro)
            if '.s3.amazonaws.com' in parsed_url.netloc:
                bucket_name = parsed_url.netloc.split('.s3.amazonaws.com')[0]
            else:
                return None, None
            
            # Extract S3 key from path (remove leading slash and decode URL encoding)
            s3_key = unquote(parsed_url.path.lstrip('/'))
            
            return bucket_name, s3_key
            
        except Exception as e:
            print(f"Error parsing S3 URL {s3_url}: {e}")
            return None, None
