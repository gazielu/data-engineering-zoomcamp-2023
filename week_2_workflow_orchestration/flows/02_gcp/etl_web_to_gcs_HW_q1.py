from pathlib import Path, PurePath, PurePosixPath
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint




@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url) 
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    print(f"log print rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = PurePosixPath("week_2_workflow_orchestration","data",color,f"{dataset_file}.parquet")
    #path = os.path.join("data",color,f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path



@task()
def create_local_folders(color: str) -> None:
    """ Create data folder for color if not existing"""
    outdir = os.path.join('week_2_workflow_orchestration','data', color)
    if not os.path.exists(outdir):
        os.mkdir(outdir)


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    #gcs_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-prefect-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""

    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)

    df_clean = clean(df)
    #create_local_folders(color)
    path = write_local(df_clean, color, dataset_file)
    # path = path.replace(r'\',r'//')
    # print(path)
    #path = r"data/green/green_tripdata_2020-01.parquet"
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()

