FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY data_pipeline.py data_pipeline.py
ENTRYPOINT [ "python" , "data_pipeline.py" ]

