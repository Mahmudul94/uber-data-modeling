import json
import boto3
from datetime import datetime
import io
from io import StringIO
import string
import pandas as pd
import csv

s3 = boto3.client('s3')
Bucket = "dw-snowflakes-mahmudul"
def datetime_dim(df):
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    
    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    return datetime_dim
    
def passenger_trip (df):
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    return passenger_count_dim
    

def trip_distance(df):  
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
    return trip_distance_dim
    
def rate_code_type(df):
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
        
}

    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]
    return rate_code_dim

def pickup_location_dim(df):
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 
    
    return  pickup_location_dim
    
def dropoff_location_dim(df):
    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]
    
    return dropoff_location_dim
    
def payment_type(df):
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
}
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
    
    return payment_type_dim
    
def process_csv(processName, df,s3 ):
    filename="raw_data/to_processed/"+processName+"/"+processName+"_.csv"
    df_data = StringIO()
    df.to_csv(df_data)
    df_content = df_data .getvalue()
    s3.put_object(Bucket=Bucket, Key=filename,Body=df_content)

def lambda_handler(event, context):
    Key = "raw_data/processed/uber_data.csv"
    response = s3.get_object(Bucket=Bucket, Key=Key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_content))
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index
    
    pickup_dropoff = datetime_dim(df)
    passenger_count_dim = passenger_trip(df)
    trip_distance_dim  = trip_distance(df)
    rate_code = rate_code_type(df)
    pickup_location = pickup_location_dim(df)
    dropoff_location = dropoff_location_dim(df)
    payment = payment_type(df)
    
    # pickup_key = "raw_data/to_processed/pickup_dropoff/pickup_dropoff_"+".csv"
    process_csv("pickup_dropoff", pickup_dropoff,s3)
    
    # pickup_dropoff_data = StringIO()
    # pickup_dropoff.to_csv(pickup_dropoff_data)
    # pickup_dropoff_content = pickup_dropoff_data .getvalue()
    # s3.put_object(Bucket=Bucket, Key=pickup_key,Body=pickup_dropoff_content)
    
    passenger_key = "raw_data/to_processed/passenger_trip/passenger_trip_"+".csv"
    passenger_count_data = StringIO()
    passenger_count_dim.to_csv(passenger_count_data)
    passenger_trip_content = passenger_count_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=passenger_key,Body=passenger_trip_content)
    
    trip_key = "raw_data/to_processed/trip_distance/trip_distance_"+".csv"
    trip_distance_data = StringIO()
    trip_distance_dim.to_csv(trip_distance_data)
    trip_distance_content = trip_distance_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=trip_key,Body=trip_distance_content)
    
    rate_code_key = "raw_data/to_processed/rate_code/rate_code_"+".csv"
    rate_code_data = StringIO()
    rate_code.to_csv(rate_code_data)
    rate_code_content = rate_code_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=rate_code_key,Body=rate_code_content)
    
    pickup_location_key = "raw_data/to_processed/pickup_location/pickup_location_"+".csv"
    pickup_location_data = StringIO()
    pickup_location.to_csv(pickup_location_data)
    pickup_location_content = pickup_location_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=pickup_location_key,Body=pickup_location_content)
    
    dropoff_location_key = "raw_data/to_processed/dropoff_location/dropoff_location_"+".csv"
    dropoff_location_data = StringIO()
    dropoff_location.to_csv(dropoff_location_data)
    dropoff_location_content = dropoff_location_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=dropoff_location_key,Body=dropoff_location_content)
    
    payment_key = "raw_data/to_processed/payment/payment_"+".csv"
    payment_data = StringIO()
    payment.to_csv(payment_data)
    payment_content = payment_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=payment_key,Body=payment_content)
    
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(rate_code, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_location, left_on='trip_id', right_on='pickup_location_id') \
             .merge(dropoff_location, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(pickup_dropoff, left_on='trip_id', right_on='datetime_id') \
             .merge(payment, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
              'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
              'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
              'improvement_surcharge', 'total_amount']]
        
    fact_key = "raw_data/to_processed/fact_table/fact_table_"+".csv"
    fact_table_data = StringIO()
    fact_table.to_csv(fact_table_data)
    fact_table_content = fact_table_data.getvalue()
    s3.put_object(Bucket=Bucket, Key=fact_key,Body=fact_table_content)