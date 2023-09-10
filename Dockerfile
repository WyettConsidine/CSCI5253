FROM python:latest

RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipeline.py
COPY hapiness_report_2022.csv hpr2022.csv

ENTRYPOINT [ "python", "pipeline.py", "hpr2022.csv", "sortedHPR2022.csv" ]
