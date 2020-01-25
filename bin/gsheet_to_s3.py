from __future__ import print_function
import os
import pickle
import json

from google.auth.transport.requests import Request
from apiclient.discovery import build
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
import boto3
import botocore


def get_google_sheet(spreadsheet_id, range_name):
    """ Retrieve sheet data using Google Python API. """
    scopes = 'https://www.googleapis.com/auth/spreadsheets.readonly'

    # Get creds
    if os.path.exists('../creds/token.pickle'):
        with open('../creds/token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../creds/credentials.json', scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('../creds/token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    # Setup the Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    gsheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    return gsheet


def gsheet2df(gsheet):
    """ Converts Google sheet data to a Pandas DataFrame.
    Note: This script assumes that your data contains a header file on the first row!
    Also note that the Google API returns 'none' from empty cells - in order for the code
    below to work, you'll need to make sure your sheet doesn't contain empty cells,
    or update the code to account for such instances.
    """
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = [row[:len(header)] for row in gsheet.get('values', [])[1:]]  # Everything else is data.
    if not values:
        print('No data found.')
    else:
        df = pd.DataFrame(values, columns=header)
        return df


print("Loading spreadsheet ID")
config = json.load(open("../creds/config.json"))

print("Getting google sheet...")
gsheet = get_google_sheet(config["spreadsheet_id"], config["range_id"])
print("Creating Dataframe")
df = gsheet2df(gsheet)
print('Dataframe size = ', df.shape)

print("Writing to S3")
bucket_name = config["data_bucket_name"] 
client = boto3.client('s3')
try:
    response = client.head_bucket(
        Bucket=bucket_name,
    )
except botocore.exceptions.ClientError:
    print("S3 bucket doesn't exist, creating...")
    response = client.create_bucket(
       Bucket=bucket_name,
       CreateBucketConfiguration={
       'LocationConstraint': 'eu-west-1'
       },
    )
    print(response)

object_name = config["data_object_name"]
s3_url = f"s3://{bucket_name}/{object_name}" 
df.to_parquet(s3_url, index=False)
