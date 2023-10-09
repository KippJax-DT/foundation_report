import boto3
import pandas as pd
import json
from io import StringIO
import os
import time
import logging

def lambda_handler(event, context):
    
    #  I first read the csv, perform actions on table as if it was a dataframe,\
    # then deposit the dataframe as a csv to the enrollment files bucket under the name foundation.csv
    # step 1 is being able to read the table in the first place. Initiate aws boto3 client
    #  step 2, download file and be able to read file in the directory
    #  step 3, perform actions on csv file as a dataframe
    #  step 4, initiate deposit to s3 bucket
    # step 5 if id like I may deposit the table itself to our dataframe just in case
    
    s3 = boto3.client('s3')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    bucket_name = os.environ.get('bucket_name')
    
    enrollment_path = os.environ.get('foundation_path')
    
    logger.info(f"file: {enrollment_path}")
    logger.info(f"Reading Bucket: {bucket_name}")
    
    enr_object = s3.get_object(Bucket = bucket_name, Key = enrollment_path)
    enr_body = enr_object['Body'].read().decode('utf-8')
    
    # Create Stringio reading object
    
    enr_io = StringIO(enr_body)
    
    enroll_df = pd.read_csv(enr_io)
    
    #change column name to remove space
    enroll_df.rename(columns={'Student ID':'student_id'}, inplace=True)
    enroll_df.rename(columns={'Last':'last'}, inplace=True)
    enroll_df.rename(columns={'first':'first'}, inplace=True)
    enroll_df.rename(columns={'Counselor':'school_id'}, inplace=True)
    enroll_df.rename(columns={'Enrollment Start Date':'enrollment_start_date'}, inplace=True)
    enroll_df.rename(columns={'Drop Date':'enrollment_drop_date'}, inplace=True)
    enroll_df.rename(columns={'Enrollment Code':'enrollment_code'}, inplace=True)
    enroll_df.rename(columns={'Drop Code':'drop_code'}, inplace=True)
    enroll_df.rename(columns={'Grade':'grade'}, inplace=True)
    enroll_df.rename(columns={'Gender':'gender'}, inplace=True)
    enroll_df.rename(columns={'Birthdate':'birthdate'}, inplace=True)
    enroll_df.rename(columns={'Ethnicity: Hispanic or Latino':'hispanic_latino'}, inplace=True)
    enroll_df.rename(columns={'Race: American Indian or Alaska Native':'american_indian_alaska_native'}, inplace=True)
    enroll_df.rename(columns={'Race: Asian':'asian'}, inplace=True)
    enroll_df.rename(columns={'Race: Black or African American':'black_african_american'}, inplace=True)
    enroll_df.rename(columns={'Race: Native Hawaiian or Other Pacific Islander':'native_hawaiian_pacific_islander'}, inplace=True)
    enroll_df.rename(columns={'Race: White':'white'}, inplace=True)
    enroll_df.rename(columns={'Most Frequently Spoken Language Student':'most_frequently_spoken_language'}, inplace=True)
    enroll_df.rename(columns={'English Language Learner':'english_language_learner'}, inplace=True)
    enroll_df.rename(columns={'School Year':'school_year'}, inplace=True)
    enroll_df.rename(columns={'Florida Education Identifier':'fleid'}, inplace=True)
    enroll_df.rename(columns={'Primary ESE':'primary_ese'}, inplace=True)
    enroll_df.rename(columns={'School Number':'school_number'}, inplace=True)
    enroll_df.rename(columns={'School':'school'}, inplace=True)

    print(f'Total Records After Column Rename: {enroll_df.shape[0]}')
    #Remove = and "" from data
    enroll_df = enroll_df.replace('=','', regex=True)
    enroll_df = enroll_df.replace('"','', regex=True)
    enroll_df = enroll_df.replace(',','', regex=True)

    print(f'Total Records after Clearing Chars (=,"): {enroll_df.shape[0]}')
    enroll_df['enrollment_start_date']= pd.to_datetime(enroll_df['enrollment_start_date'])
    enroll_df['enrollment_drop_date']= pd.to_datetime(enroll_df['enrollment_drop_date'])


    #reformat school year
    enroll_df.school_year.replace('2023-2024','2024', inplace=True)
    #drop non-numeric values
    enroll_df.school_id.replace('[^0-9]','', inplace=True)
    print(f'Total Records after original years, and non-numerics removed: {enroll_df.shape[0]}')
    #enroll_df.drop_duplicates(subset=['student_id'], keep='first', inplace=True)
    print(f'Total Records after dropping duplicates: {enroll_df.shape[0]}')
    
    csv_buffer = StringIO()
    
    enroll_df.to_csv(csv_buffer)
    
    s3_resource = boto3.resource('s3')
    upload_bucket = os.environ.get('upload_bucket')
    uploaded_name = os.environ.get("upload_file_name")
    
    s3_resource.Object(upload_bucket, uploaded_name).put(Body=csv_buffer.getvalue())
    
    print("foundation Report under foundation.csv under enrollment files bucket has been uploaded")
    
    