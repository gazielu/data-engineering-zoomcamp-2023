"""
Hoome work 03
based on parameterized_flow_hw_q3

hw link 
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_3_data_warehouse/homework.md
data
https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv
"""



from pathlib import Path, PurePosixPath
import numpy as np 
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read fhv_tripdata from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, encoding='windows-1252')
    return df


@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    return df

@task(log_prints=True)
def create_local_folders(color: str) -> None:
    """ Create data folder for color if not existing"""
    outdir = os.path.join('data', color)
    print(outdir)
    if not os.path.exists(outdir):
        os.mkdir(outdir)

@task(log_prints=True)
def write_local( df: pd.DataFrame, dataset_file: str): #-> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.csv.gz")   
    df.to_csv(path, compression="gzip")
    path = PurePosixPath("data",color,f"{dataset_file}.csv.gz") # gcp path
    print(path)
    


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("green-tripdata")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return 1


@flow(log_prints=True, name="Subflow ETL" )
def etl_web_to_gcs(year: int, month: int, color: str) -> int:
    """The main ETL function"""
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    
    print(f"Log print: Start extracting file  : {dataset_file}")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    

    df = fetch(dataset_url)
    df_clean = clean(df)
    create_local_folders(color)
    write_local(df_clean, dataset_file)
    path = PurePosixPath("data",color,f"{dataset_file}.csv.gz")
    write_gcs(path)


@flow(name="Parent ETL", log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: list[int] = [2019, 2020], color: str = "green"
): 
    """ main """
    file_list = []
    df_total_count = 0
    
    for year in year:
        for month in months:
            df_file_count = etl_web_to_gcs(year, month, color)
            file_list.append([f"{year}-{month}", df_file_count])
    #print(f"Log print: Number rows in all files : {df_file_count}")
    #file_count = pd.DataFrame(file_list, columns=['year-month', 'row-count'])
    #path = Path("data/log.csv")  
    #file_count.to_csv(path,index=False)

if __name__ == "__main__":
    color = "green"
    months = list(np.arange(1, 13))
    year = [2019,2020]
    etl_parent_flow(months, year,color)

