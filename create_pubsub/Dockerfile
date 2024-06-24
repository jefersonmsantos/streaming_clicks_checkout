FROM python:3.10-slim

WORKDIR /opt/create_bigquery_table

COPY requirements.txt /opt/create_bigquery_table/

RUN pip install --no-cache-dir -r /opt/create_bigquery_table/requirements.txt

CMD ["tail" "-F" "anything"]