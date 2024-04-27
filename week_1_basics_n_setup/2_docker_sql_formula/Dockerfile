FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data_races.py ingest_data_races.py 

ENTRYPOINT [ "python", "ingest_data_races.py" ]