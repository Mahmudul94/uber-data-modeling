#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd


# In[12]:


df = pd.read_csv("uber_data.csv")


# In[13]:


df


# In[14]:


df. info()


# In[15]:


df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])


# In[16]:


df.info()


# In[17]:


df = df.drop_duplicates().reset_index(drop=True)
df['trip_id'] = df.index


# In[9]:


df


# In[28]:


datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
datetime_dim['pick_hour']=datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pick_day']=datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pick_month']=datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pick_year']=datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pick_weekday']=datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['pick_hour']=datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['pick_day']=datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['pick_month']=datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['pick_year']=datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['pick_weekday']=datetime_dim['tpep_dropoff_datetime'].dt.weekday


# In[29]:


datetime_dim['datetime_id']=datetime_dim.index


# In[30]:


datetime_dim = datetime_dim[['datetime_id','tpep_pickup_datetime','pick_hour','pick_day','pick_month',
                            'pick_year','pick_weekday','tpep_dropoff_datetime','pick_hour','pick_day','pick_month',
                            'pick_year','pick_weekday']]


# In[31]:


datetime_dim


# In[32]:


passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
passenger_count_dim['passenger_count_id']=passenger_count_dim.index
passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

trip_distance_dim =df[['trip_distance']].drop_duplicates().reset_index(drop=True)
trip_distance_dim['trip_distance_id']=trip_distance_dim.index
trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]


# In[42]:


rate_code_type = {
    1:"standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau and Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}
rate_code_dim = df[["RatecodeID"]].drop_duplicates().reset_index(drop=True)
rate_code_dim['rate_code_id'] = rate_code_dim.index
rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)


# In[45]:


pickup_location_dim = df[['pickup_longitude','pickup_latitude']].drop_duplicates().reset_index(drop=True)
pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']]

dropoff_location_dim = df[['dropoff_longitude','dropoff_latitude']].drop_duplicates().reset_index(drop=True)
dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]


# In[46]:


payment_type_name ={
    1:"credit card",
    2:"cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
}
payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
payment_type_dim['payment_type_id'] = payment_type_dim.index
payment_type_dim['payment_type_name']=payment_type_dim['payment_type'].map(payment_type_name)
payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]


# In[47]:


payment_type_dim


# In[48]:


fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id')              .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id')              .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id')              .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id')              .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id')              .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id')              [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]


# In[51]:


fact_table


# In[ ]:




