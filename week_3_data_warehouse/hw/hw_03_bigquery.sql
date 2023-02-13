--- create extranl table

CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://fhv-vehicle/data/fhv_tripdata_2019-*.csv.gz']
);

--************   Q1    ******************

SELECT count(*) FROM `stoked-duality-375907.dezoomcamp.fhv_tripdata`
-- ANSWER 43244696
-- 0 bytes processed / billed

CREATE OR REPLACE TABLE dezoomcamp.fhv_tripdata_np AS
SELECT * FROM `stoked-duality-375907.dezoomcamp.fhv_tripdata`;

-- 3.03 GB processed

--************   Q2    ******************
-- bigquery table
select count (distinct affiliated_base_number) from dezoomcamp.fhv_tripdata_np
-- 317.84 bytes proceeses  / 318 billed

-- extranel table
0 bytes  processed data estimate
3 gb in actual

-- ANSWER 0 MB for the External Table and 317.94MB for the BQ Table

-- ********** Q3    ************ 

select count(*)  from dezoomcamp.fhv_tripdata_np
where PUlocationID is null
and DOlocationID is null


-- ANSWER > 717748

-----************ Q4 **********

Partition by pickup_datetime Cluster on affiliated_base_number


--- ********* q5 - ****************
-- non partition
select count( distinct affiliated_base_number)  from dezoomcamp.fhv_tripdata_np
where pickup_datetime between '2019-03-01' and '2019-03-31';
-- 647.87 MB non partiton table 

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dezoomcamp.fhv_tripdata_partition_cluster
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `dezoomcamp.fhv_tripdata`;

select count( distinct affiliated_base_number)  from dezoomcamp.fhv_tripdata_partition_cluster
where pickup_datetime between '2019-03-01' and '2019-03-31';
-- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

-- *************** Q6 ************
Where is the data stored in the External Table you created?
GCP Bucket

--- ************* q7 ***********

It is best practice in Big Query to always cluster your data:
False - less the 1 GB its can cost due to meta data read ad metadata maintanceses so its better not to partition and cluster

