import boto3
import csv
from . import settings
from botocore.exceptions import ClientError
from botocore.client import Config

s3_client = boto3.client(
    's3',
    aws_access_key_id=settings.ACCESS_KEY_ID,
    aws_secret_access_key=settings.ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4'))
s3_resource = boto3.resource(
    's3',
    aws_access_key_id=settings.ACCESS_KEY_ID,
    aws_secret_access_key=settings.ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4'))

# TODO convert to classes and objects for better referencing


def s3_presigned_url(path):
    """ .:e:.
    Args:
        path:
    """
    try:
        if path[0] == "/":
            path = path[1:]  # Cut away the preceding "/" so it saves to the correct location
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': settings.BUCKET_NAME,
                'Key': path
            },
            ExpiresIn=3600,
            HttpMethod="GET")

        return url
    except Exception as e:
        print((str(e)))
        return 'error'


def s3_file_exists(url):
    """ .:e:.
    Check to see if the file exists on s3 bucket
    Args:
        url:
    """
    try:
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        s3_resource.Object(settings.BUCKET_NAME, url).load()
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something has gone wrong.
            raise
    else:
        return True


def s3_save_to_bucket(data, url, content_type=None):
    """ .:e:.
    Args:
        data:
        url:
    """
    try:
        # Image Uploaded
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        s3_resource.Bucket(settings.BUCKET_NAME).put_object(Key=url, Body=data, ACL='private', ContentType=content_type)
    except Exception as e:
        print((str(e)))


def s3_list_all_objects(url):
    """ .:e:.
    Args:
        url:
    """
    try:
        # Image Uploaded
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        items = []
        bucket = s3_resource.Bucket(settings.BUCKET_NAME)
        for item in bucket.objects.filter(Prefix=url):
            if str(item.key[-1]) != "/":
                items.append(item.key)
        return items
    except Exception as e:
        print((str(e)))


def s3_get_from_bucket(url):
    """ .:e:.
    Gets and returns a file from s3 bucket
    Args:
        url: File location inside s3
    """
    try:
        # Image Uploaded
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        obj = s3_resource.Object(settings.BUCKET_NAME, url)
        obj = obj.get()['Body'].read()
        return obj
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something has gone wrong.
            raise
    except Exception:
        raise


def s3_copy_file(old_url, new_url):
    """ .:e:.
    Copies a file to the new_url in s3 bucket
    Args:
        old_url: Original location inside s3
        new_url: New location inside s3
    Returns:
        True if successful, False if unsuccessful
    """
    try:
        # Image Uploaded
        if old_url[0] == "/":
            old_url = old_url[1:]  # Cut away the preceding "/" so it saves to the correct location
        if new_url[0] == "/":
            new_url = new_url[1:]  # Cut away the preceding "/" so it saves to the correct location
        s3_resource.Object(settings.BUCKET_NAME, new_url).copy_from(CopySource=settings.BUCKET_NAME + '/' + old_url)
        return True
    except Exception:
        return False


def s3_delete_file(url):
    """ .:e:.
    Deletes a file in the aws s3 bucket
    Args:
        url: File location inside s3
    Returns:
        True if successful, False if unsuccessful
    """
    try:
        # Image Uploaded
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        s3_resource.Object(settings.BUCKET_NAME, url).delete()
        return True
    except Exception:
        return False


def s3_save_csv(headers, rows, url):
    """ .:e:.
    Args:
        data:
        url:
    """
    try:
        url = 'media/' + url
        # Add headers to CSV
        index = 0
        csvContent = ''
        for key in headers:
            index = index + 1
            csvContent += (key + ',') if (index < len(headers)) else (key + "\n")
        
        i = 0
        for row in rows:
            i = i + 1
            dataString = ''
            for key in headers:
                if (row[key.lower()]):
                    # Add leading quotation mark
                    dataString += '"'
                    dataString += str(row[key.lower()])
                    # Add trailing quotation mark
                    dataString += '"'
                dataString += ','
            dataString = dataString[:-1]

            csvContent += dataString + "\n"
        csvContent = csvContent[:-1]
        # Image Uploaded
        if url[0] == "/":
            url = url[1:]  # Cut away the preceding "/" so it saves to the correct location
        s3_resource.Bucket(settings.BUCKET_NAME).put_object(Key=url, Body=csvContent, ACL='private')
        return True
    except Exception as e:
        print((str(e)))
        return False
