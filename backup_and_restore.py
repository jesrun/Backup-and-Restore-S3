# Name: Jessica Runandy
# Date: November 4, 2020
# Description: This application uses cloud storage APIs to recursively traverse
# the files of a directory, makes a backup to a bucket in S3, and restores from
# a bucket in S3.

# References:
# https://zihao.me/post/calculating-etag-for-aws-s3-objects/
# https://stackoverflow.com/questions/31918960/boto3-to-download-all-files-from-a-s3-bucket
# https://www.developerfiles.com/upload-files-to-s3-with-python-keeping-the-original-folder-structure/


import boto3
import os
from botocore.client import ClientError
import hashlib
import sys


def md5_checksum(absolute_path):
    """Calculates the checksum of a file using its absolute path.

    Parameters
    ----------
    absolute_path : string
        The absolute path of the local file

    Returns
    ----------
    string
        Checksum value of the file

    """
    m = hashlib.md5()
    with open(absolute_path, 'rb') as f:
        for data in iter(lambda: f.read(1024 * 1024), b''):
            m.update(data)
    return m.hexdigest()


def exists(upload_name, buckets, s3):
    """Determines if the local file or directory exists in S3.

    Parameters
    ----------
    upload_name : string
        The file name to be uploaded into the bucket
    buckets : {__iter__}
        All of the buckets the user has in S3
    s3 : {Object}
        The s3 resource

    Returns
    ----------
    s3_object
        Local file exists on the cloud
    None
        Local file does not exist on the cloud

    """

    # Loop through the buckets in S3
    for bucket in buckets:
        try:
            # Checks if the object exists in S3
            s3.Object(bucket.name, upload_name).load()
        except ClientError:
            # Continue to loop through all the buckets in S3
            continue
        else:
            # Object exists in S3
            s3_client = boto3.client('s3')
            objects_list = s3_client.list_objects_v2(Bucket=bucket.name)[
                'Contents']

            # Loop through the objects in a specific bucket (the bucket that
            # contains the duplicated file on the cloud)
            for s3_object in objects_list:

                # Splits the key into head and tail to check if file exists
                # on cloud
                # Ex: if file is stored as subdirectory/file1,
                # file1 is extracted to be used as a comparison
                # tail: the last path component
                # head: everything leading up to the tail
                head, tail = os.path.split(s3_object['Key'])

                upload_name_head, upload_name_tail = os.path.split(upload_name)

                # If name of the object in the S3 bucket is equal to the object
                # that the user would like to backup, return the object
                if tail == upload_name_tail:
                    return s3_object

            return None


def check_etag(absolute_path, duplicate_object):
    """Determines if the local file has been modified locally since it was last
    backed up.

    Parameters
    ----------
    absolute_path : string
        The absolute path of the file to be uploaded into the bucket
    duplicate_object : {__getitem__}
        The duplicate object

    Returns
    ----------
    bool
        True if file has been modified locally since it was last backed up,
        False otherwise.

    """
    etag = md5_checksum(absolute_path)

    # S3 stores etag in quotes, so need to remove the quotes
    unquoted_etag = duplicate_object['ETag'][1:-1]

    # If the file has been modified locally since it was last backed up,
    # the Etag of the local file would not match the Etag of the object on
    # the cloud.
    if etag != unquoted_etag:
        return True
    else:
        # Object has not been modified locally since it was last backed up
        return False


def backup_to_s3(directory_path, s3_resource, bucket_name):
    """Backup local files and/or subdirectories to the specified S3 bucket.

    The directory structure of the files is respected and visible in the
    cloud. If a directory or file already exists on the cloud and it has not
    been modified locally since it was backed up, it will not backup the
    directory or file.

    Parameters
    ----------
    directory_path : string
        User input path of the directory, can be relative or absolute path
    s3_resource : {Object}
        The s3 resource
    bucket_name : string
        The bucket name inputted by user

    """
    new_bucket = s3_resource.Bucket(bucket_name)
    buckets = s3_resource.buckets.all()

    # Absolute path of the directory inputted by the user
    directory_abs_path = os.path.abspath(directory_path)

    client = boto3.client('s3')

    # Loops through the sub directory and files in the specified directory
    # inputted by the user
    for subdirectory, directory, files in os.walk(directory_abs_path):
        subdirectory_abs_path = os.path.abspath(subdirectory)
        if len(os.listdir(subdirectory_abs_path)) == 0:
            if subdirectory_abs_path == directory_abs_path:
                # Directory is empty
                head, tail = os.path.split(subdirectory_abs_path)
                tail = tail + '/'
                duplicate_object = exists(tail, buckets, s3_resource)
                if duplicate_object is None:
                    client.put_object(Bucket=bucket_name, Key=tail)
                    print("Uploaded directory " + tail)
                else:
                    print("Directory " + tail + " was not uploaded (has "
                                                "not been modified "
                                                "locally since it was "
                                                "backed up).")
            else:
                # Subdirectory is empty
                # Update upload_name to include the original directory name
                # and the name of the sub_directory
                # Ex: if directory name is "directory" and subdirectory name is
                # "subdirectory", "directory/subdirectory/" will be uploaded
                upload_name = subdirectory_abs_path[len(directory_abs_path)
                                                    + 1:] + '/'
                duplicate_object = exists(upload_name, buckets, s3_resource)
                if duplicate_object is None:
                    client.put_object(Bucket=bucket_name, Key=upload_name)
                    print("Uploaded directory " + upload_name)
                else:
                    print("Directory " + upload_name + " was not uploaded (has "
                                                       "not been modified "
                                                       "locally since it was "
                                                       "backed up).")

        for file_name in files:
            file_absolute_path = os.path.join(subdirectory_abs_path, file_name)

            # The file or directory name to be uploaded (keeps the directory
            # structure of the files and is visible in the cloud)
            # Ex: if there is a sub directory called "sub_directory" in the
            # directory inputted by the user and "file1" is in the
            # "sub_directory", it will upload as "sub_directory/file1"
            upload_name = file_absolute_path[len(directory_abs_path) + 1:]

            # Checks if the file exists on S3. duplicate_object stores the
            # object if there is a duplicate, "None" if there is no duplicate
            duplicate_object = exists(upload_name, buckets, s3_resource)

            # If the file does not exist on S3
            if duplicate_object is None:
                # Backup the local file to the specified bucket while keeping
                # the original folder structure
                with open(file_absolute_path, 'rb') as data:
                    new_bucket.put_object(Key=upload_name, Body=data)

                print("Uploaded file " + upload_name)

            else:
                # The file exists on S3, calls check_etag to check if the
                # local file has been modified locally since it was last
                # backed up. has_been_modified is true if the local file has
                # been modified.
                has_been_modified = check_etag(file_absolute_path,
                                               duplicate_object)

                if has_been_modified:
                    # Backup the modified local file to the specified bucket
                    with open(file_absolute_path, 'rb') as data:
                        new_bucket.put_object(Key=upload_name, Body=data)
                    print("Uploaded file " + upload_name)

                else:
                    print("File " + upload_name + " was not uploaded (has not "
                                                  "been modified locally "
                                                  "since it was backed up).")


def restore_from_s3(s3_resource, client, directory_path, bucket_name, prefix,
                    paginator):
    """Restore from a S3 bucket in the the cloud to the specified directory. The
    directory structure of the files is respected.

    Parameters
    ----------
    s3_resource : {Object}
        The s3 resource
    client: BaseClient
        The s3 client
    directory_path : string
        User input path of their directory, can be relative or absolute path
    bucket_name : string
        The bucket name inputted by user
    prefix: string
        The key of the object in the bucket
    paginator: Paginator
        The paginator object from the list_objects_v2 operation of S3

    """
    for page_iterator in paginator.paginate(Bucket=bucket_name, Delimiter='/',
                                            Prefix=prefix):
        if page_iterator.get('CommonPrefixes') is not None:
            for subdirectory in page_iterator.get('CommonPrefixes'):
                restore_from_s3(s3_resource, client, directory_path,
                                bucket_name, subdirectory.get('Prefix'),
                                paginator)
        for file in page_iterator.get('Contents', []):
            destination_path = os.path.join(directory_path, file.get('Key'))
            if not os.path.exists(os.path.dirname(destination_path)):
                # Create the directory if it does not exist
                os.makedirs(os.path.dirname(destination_path))
                if os.path.isdir(destination_path):
                    print("Created directory " + file.get('Key'))
            if not file.get('Key').endswith('/'):
                # Download the file if it's not a directory
                s3_resource.meta.client.download_file(bucket_name,
                                                      file.get('Key'),
                                                      destination_path)
                print("Downloaded file " + file.get('Key'))


def main():
    arguments = len(sys.argv) - 1
    if arguments != 3:
        print("ERROR: Invalid number of arguments.")
        return

    if sys.argv[1] == "backup":
        directory_path = sys.argv[2]
        bucket_name = sys.argv[3]
    elif sys.argv[1] == "restore":
        directory_path = sys.argv[3]
        bucket_name = sys.argv[2]
    else:
        print("ERROR: Invalid command.")
        return

    s3_resource = boto3.resource("s3")

    if sys.argv[1] == "backup":
        # Path of the directory does not exist (for relative and absolute path)
        if not os.path.isdir(directory_path):
            print("ERROR: Directory does not exist.")
            return

        try:
            # Check if the bucket exists and the user has access to it
            s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError:
            # Bucket does not exist or user does not have access to it
            try:
                # Create session to create the bucket based on the user's
                # credentials (location)
                session = boto3.session.Session()
                current_region = session.region_name

                s3_resource.create_bucket(Bucket=bucket_name,
                                          CreateBucketConfiguration={
                                              'LocationConstraint': current_region})
                print("Created bucket " + bucket_name)

            except ClientError:
                # Unable to create a new bucket due to invalid bucket name
                # (name did not follow the rules for naming S3 buckets)
                print("ERROR: Invalid bucket name.")
                return

        print("Backing up from directory " + directory_path + " into bucket "
              + bucket_name + "...")
        backup_to_s3(directory_path, s3_resource, bucket_name)
        print("SUCCESS: Backup from " + directory_path + " into bucket "
              + bucket_name)
        return

    else:
        try:
            # Check if the bucket exists and the user has access to it
            s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError:
            # Bucket does not exist or user does not have access to it
            print("ERROR: Invalid bucket name.")
            return

        buckets = s3_resource.buckets.all()
        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects_v2')
        print("Restoring from bucket " + bucket_name + " into directory "
              + directory_path + "...")
        for bucket in buckets:
            if bucket.name == bucket_name:
                # Loops through and restore each object in the bucket
                for s3_object in bucket.objects.all():
                    restore_from_s3(s3_resource, client, directory_path,
                                    bucket_name, s3_object.key, paginator)
                print("SUCCESS: Restore from bucket " + bucket_name + " into "
                      + directory_path)
                break


# Calls main
if __name__ == "__main__": main()
