# Use an official Python runtime as a parent image
FROM python:3.8

WORKDIR /app
COPY process_csv.py process_csv.py
COPY Test.csv Test.csv
COPY output.csv output.csv
RUN pip install pandas
RUN python process_csv.py Test.csv output.csv
ENTRYPOINT [ "bash" ]