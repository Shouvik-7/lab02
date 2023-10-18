FROM python:3.11

WORKDIR /app

COPY pipeline.py pipeline.py
COPY Austin_Animal_Center_Outcomes_20231013.csv data.csv
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
RUN pip install psycopg2

ENTRYPOINT ["python", "pipeline.py"]
