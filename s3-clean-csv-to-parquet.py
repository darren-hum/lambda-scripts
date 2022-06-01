### To ingest csv files unzipped from vendor; clean column headers; convert to parquet
### Files/Folders have spacing issues and encoded by aws
### Column headers have special characters that might be problematic

import json
import os
import io
import boto3
import csv
import pandas as pd
import logging
import pyarrow
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3 = boto3.client('s3')

def clean_text(text):
    """ removes special chars """
    specialchars = "!@#$%^&*()[]{};:,./<>?\|`~-=_+\""
    replacechars = "                               " #corresponding characters in inCharSet to be replaced
    trans_dict = str.maketrans(specialchars, replacechars)
    text = text.translate(trans_dict)

    return text
    
def parse_url(text):
    """ function parses key to remove encoding """
    text = unquote_plus(text)

    return text

def filename_rm_space(filename):
    """ returns file name without spaces """
    filename = filename.split('/')[2]
    filename_parts = filename.split('(')
    part1 = filename_parts[0].strip().replace(" ", "_")
    part2 = filename_parts[1].strip().replace(" ", "")
    pq_filename = pq1 + "(" + pq2 + ".parquet"

    return pq_filename

def lambda_handler(event, context):
    """ Event: Object Creation in S3 """
    logger.info(event)
    
    if event:
        # get all the params needed
        file_obj = event["Records"][0]
        bucket_name = file_obj['s3']['bucket']['name']
        file_name = file_obj['s3']['object']['key']
        file_name = unquote_plus(file_name)
        
        # for debugging
        logger.info(f"File_obj: {file_obj}")
        logger.info(f"bucket_name: {bucket_name}") 
        logger.info(f"file_name: {file_name}")
        
        #reads the csv into df
        fileObject = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(io.BytesIO(fileObject['Body'].read()))
        
        # clean column headers
        df.columns = [clean_text(x) for x in df.columns]
        
        # provide clean filename and save as parquet
        pq_filename = filename_rm_space(file_name)
        s3_path = "s3://dev-sim-input-data/smile_data/converted/" + pq_filename
        df.to_parquet(s3_path, compression='snappy')

