import json
import os
import io
import boto3
import csv
import pandas as pd
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3 = boto3.client('s3')

def clean_text(text):

    specialchars = "!@#$%^&*()[]{};:,./<>?\|`~-=_+\""
    replacechars = "                               " #corresponding characters in inCharSet to be replaced
    trans_dict = str.maketrans(inCharSet, outCharSet)
    text = text.translate(splCharReplaceList)

    return text

def lambda_handler(event, context):
    """ Read file from s3 on new file in folder"""
    
    s3 = boto3.client('s3')
    if event:
        file_obj = event["Records"][0]
        bucket_name = file_obj['s3']['bucket']['name']
        file_name = file_obj['s3']['object']['key']
        
        logger.info(f"File_obj: {file_obj}")
        logger.info(f"bucket_name: {bucket_name}") 
        logger.info(ffile_name: {file_name}")
        
        pq_file_name = file_name.split('/')[2] + ".parquet"
 
        fileObject = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(io.BytesIO(fileObject['Body'].read()))
        
        # clean column headers
        df.columns = [clean_text(x) for x in df.columns]
        s3_path = "s3://dev-sim-input-data/smile_data/converted/" + pq_file_name
        df.to_parquet(s3_path, compression='snappy')

