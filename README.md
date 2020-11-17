# Backup-and-Restore-S3
This program is a command-line program that recursively traverses through the files of a directory to make a backup to a bucket in Amazon S3 and to restore from a bucket in Amazon S3 to a directory.

## Dependencies
Setup AWS CLI version 2: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

Download Python 3: https://www.python.org/downloads/

Download pip3 for python3: `sudo apt-get install python3-pip`

Install boto3 using pip3: `pip3 install boto3`

## Usage (Linux)
```
$ python3 backup_and_restore.py backup <directory_path> <bucket_name>
$ python3 backup_and_restore.py restore <bucket_name> <directory_path>
```
