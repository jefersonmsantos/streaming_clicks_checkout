FROM python:3.10-slim

WORKDIR /opt/streaming

COPY requirements.txt /opt/streaming/

RUN pip install --no-cache-dir -r /opt/streaming/requirements.txt

CMD ["tail" "-F" "anything"]