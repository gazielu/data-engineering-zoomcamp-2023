CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://fhv-vehicle/data/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(*) FROM `stoked-duality-375907.dezoomcamp.fhv_tripdata`

43244696


