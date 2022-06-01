# to read uploaded csv file
# add new columnn with sentiment analysis by Amazon Comprehend
# put data into dynamodb table

import json
import os
import boto3
import csv
from boto3.dynamodb.conditions import Key, Attr

comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('hotel-reviews-sentiment')
print('Loading')

def lambda_handler(event, context):
    """ Read file from s3 on new file in folder"""
    
    s3 = boto3.client('s3')
    if event:
        file_obj = event["Records"][0]
        bucket_name = file_obj['s3']['bucket']['name']
        file_name = file_obj['s3']['object']['key']
        
        fileObject = s3.get_object(Bucket=bucket_name, Key=file_name)
        fileContent = fileObject['Body'].read().decode('utf-8-sig').splitlines(True)
        #print(fileContent)
        
        reader = csv.DictReader(fileContent)
        for row in reader:
            table.put_item(
                Item = {
                    'Hotel_Name' : row['Hotel_Name'],
                    'Date' : row['Date'],
                    'Title' : row['Title'],
                    'Review': row['Review'],
                    'Rating': row['Rating'],
                    'Sentiment': comprehend.detect_sentiment(Text=row['Review'], LanguageCode='en')['Sentiment']
                    }
                )
