FROM python:3.11

WORKDIR /app
COPY pipeline.py pipeline.py
COPY shelter1000.csv shelter1000.csv
RUN pip install numpy pandas sqlalchemy psycopg2

ENTRYPOINT [ "python", "pipeline.py" ]