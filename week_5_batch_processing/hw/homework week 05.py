# Databricks notebook source

#  # Week 5 Homework

# COMMAND ----------


#  ## pre setup
#  1. create s3 bucket <br>
#    1.1 download access keyfile
#  2. mount s3 buckets to databricks (with access key table)
#  3. download FHVHV 2021-06 to databricks filestore
#  4. copy to mount table
#  5. read table with spark read  (Copy into optional)

# COMMAND ----------

import boto3
from botocore.retries import bucket
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib


# COMMAND ----------


def mount_s3_bucket(bucket_name:str,mountpoint:str) -> None:
    '''
    this function mount desired s3 bucket to databricks DBFS
    pre-setup
    This function load pre imported aws access key table to databricks
    parameters:
    bucket_name: str
    mountpoint: the parent folder name
    '''
    
    client = boto3.client('s3')    
    # Define file type
    file_type = 'csv'
    # Whether the file has a header
    first_row_is_header = 'true'
    # Delimiter used in the file
    delimiter = ','
    # Read the CSV file to spark dataframe
    aws_keys_df = spark.read.format(file_type)\
    .option('header', first_row_is_header)\
    .option('sep', delimiter)\
    .load('/FileStore/tables/databricks_accessKeys.csv')
    
    # Get the AWS access key and secret key from the spark dataframe
    ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
    SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
    # Encode the secrete key
    ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')
    
    #mount folder in s3
    MOUNTPOINT = mountpoint # sample "/mnt/zoomcamp"
    
    if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
      dbutils.fs.unmount(MOUNTPOINT)
    
    AWS_S3_BUCKET = bucket_name # sample 'databricks-ug'
    # Mount name for the bucket
    MOUNT_NAME = mountpoint # sample "/mnt/zoomcamp"
    # Source url
    SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
    # Mount the drive
    
    dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
    
    # Check if the AWS S3 bucket was mounted successfully
    display(dbutils.fs.ls('/mnt/zoomcamp/'))
    print(SOURCE_URL)

# COMMAND ----------

mount_s3_bucket("databricks-ug","/mnt/zoomcamp")

# COMMAND ----------

# wget option
##!wget -P /tmp https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-11.parquet

# COMMAND ----------

#dbfs:/hls/fhvhv_tripdata_2021-06_test3.parquet
dbutils.fs.cp("file:/dbfs/hls/fhvhv_tripdata_2021-06_test3.parquet","/mnt/zoomcamp/fhvhv_tripdata_2021-06.parquet")

# COMMAND ----------

import urllib.request
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/fhvhv_tripdata_2021-07.csv"

file_path = "/dbfs/mnt/zoomcamp/fhvhv_tripdata_2021-07.csv"

urllib.request.urlretrieve(url, file_path)

# COMMAND ----------

import urllib.request
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
#file_path = "/dbfs/mnt/zoomcamp/fhvhv_tripdata_2021-07.parquet"
file_path = "/dbfs/mnt/zoomcamp/taxi_zone_lookup.csv"
#s3://databricks-ug/
urllib.request.urlretrieve(url, file_path)



# display file in s3
display(dbutils.fs.ls("s3://databricks-ug/zoomcamp"))

# COMMAND ----------

df = spark.read.format("parquet").load("s3://databricks-ug/fhvhv_tripdata_2021-06.parquet")
df.write.saveAsTable("fhvhv_Tripdata")


# create schema
from pyspark.sql.types import StructType, StructField,\
     FloatType, TimestampType, IntegerType, StringType, DoubleType

fhvhv_tripdata_schema = StructType([
StructField("hvfhs_license_num" , StringType() ,True),
StructField("dispatching_base_num" , StringType() ,True),  
StructField("originating_base_num" , StringType() ,True),  
StructField("request_datetime" , TimestampType() ,True),
StructField("on_scene_datetime" , TimestampType() ,True),
StructField("pickup_datetime" , TimestampType() ,True),
StructField("dropoff_datetime" , TimestampType() ,True),
StructField("PULocationID" , IntegerType() ,True),
StructField("DOLocationID" , IntegerType() ,True),
StructField("trip_miles" , DoubleType() ,True),
StructField("trip_time" , IntegerType() ,True),
StructField("base_passenger_fare" , DoubleType() ,True),  
StructField("tolls" , DoubleType() ,True),
StructField("bcf" , DoubleType() ,True),
StructField("sales_tax" , DoubleType() ,True),
StructField("congestion_surcharge" , DoubleType() ,True),
StructField("airport_fee" , DoubleType() ,True),
StructField("tips" , DoubleType() ,True),
StructField("driver_pay" , DoubleType() ,True),
StructField("shared_request_flag" , StringType() ,True),
StructField("shared_match_flag" , StringType() ,True),
StructField("access_a_ride_flag" , StringType() ,True),
StructField("wav_request_flag" , StringType() ,True),
StructField("wav_match_flag" , StringType() ,True)
])

# COMMAND ----------

fhvhv_tripdata = spark.read.format("parquet").option("schema",fhvhv_tripdata_schema).load("s3://databricks-ug/fhvhv_tripdata_2021-06.parquet")

# COMMAND ----------

# hw question 2
fhvhv_tripdata.repartition(12).write.parquet("s3://databricks-ug/zoomcamp/fhvhv_tripdata/2020/06/fhvhv_tripdata_2021-06.parquet")

# COMMAND ----------

fhvhv_tripdata.registerTempTable('fhvhv_tripdata_temp')

# COMMAND ----------

#homewiorj query 3
spark.sql("select count(*) from fhvhv_tripdata_temp where pickup_datetime between '2021-06-15 00:00:00' AND '2021-06-16 00:00:00'").show()


# COMMAND ----------

# question 4
display(spark.sql("select Max(trip_time/60/60) as trip_time_hr from fhvhv_tripdata_temp").show())

# question 5
4040


# COMMAND ----------

#add taxi zone lookup
df = spark.read \
    .option("header","true") \
    .option("inferschema","true") \
    .csv('s3://databricks-ug/taxi_zone_lookup.csv')

df.write.parquet("s3://databricks-ug/zoomcamp/fhvhv_tripdata/taxi_zone_lookup.parquet")

# COMMAND ----------

df.registerTempTable('taxi_zone_lookup')



### Question 6: 

**Most frequent pickup location zone**

spark.sql("SELECT Zone, COUNT(*) as total_rides, RANK() OVER (ORDER BY COUNT(*) DESC) as ride_rank \
            from fhvhv_tripdata_temp fhv \
            left join taxi_zone_lookup tz on tz.LocationID = fhv.PULocationID \
            GROUP BY Zone \
            ORDER BY ride_rank ASC").show()   
          
     

# COMMAND ----------


