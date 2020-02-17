import boto3
from botocore.exceptions import ClientError

def create_dir():
    """Create directory structure in the given bucket based on the US states name

    :param bucket: Bucket to create structure in it
    """
    s3 = boto3.client('s3')
    states = ['IN','IL','KS','SC','HI','GA','SD','CO','NH','MS','MD','UT','LA','ME',
        'WI','NJ','AR','NY','MT','OK','MA','NM','WY','OH','OR','NV','TX','TN','AZ',
        'MN','WA','WV','NC','MO','AL','VA','CA','CT','AK','ND','VT','MI','NE','KY',
        'ID','DC','IA','FL','PA','RI','DE'] 
    for state in states:
        dir_name = 'elasticmapreduce/csv/state='+state
        print(dir_name)
        s3.put_object(Bucket=bucket,Key=(dir_name+'/'))
        upload_file('sample-data/'+state+'.csv', bucket, dir_name+'/'+state+'.csv')


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)


bucket="" # set your bucket-name here

create_dir(bucket)
