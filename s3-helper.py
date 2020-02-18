import boto3
from botocore.exceptions import ClientError

def create_dir(bucket, ext):
    """Create directory structure in the given bucket based on the US states name

    :param bucket: Bucket to create structure in it
    :param ext: file extension to interact with
    """
    s3 = boto3.client('s3')
    states = ['IN','IL','KS','SC','HI','GA','SD','CO','NH','MS','MD','UT','LA','ME',
        'WI','NJ','AR','NY','MT','OK','MA','NM','WY','OH','OR','NV','TX','TN','AZ',
        'MN','WA','WV','NC','MO','AL','VA','CA','CT','AK','ND','VT','MI','NE','KY',
        'ID','DC','IA','FL','PA','RI','DE'] 
    for state in states:
        dir_name = 'elasticmapreduce/dbname/'+ext+'/state='+state
        print(dir_name)
        s3.put_object(Bucket=bucket,Key=(dir_name+'/'))
        upload_file('sample-data/'+ext+'/'+state+'.'+ext, bucket, dir_name+'/'+state+'.'+ext)


def upload_file(file_name, bucket, object_name=None, s3=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param s3: S3 Client to use. If not specified a new client will be created
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    if s3 is None:
        s3 = boto3.client('s3')
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)


bucket="hive-presto-sample-data-bucket" # set your bucket-name here

create_dir(bucket,'parquet')
