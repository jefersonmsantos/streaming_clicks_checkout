FROM python:3.10-slim-bullseye

WORKDIR /opt/streaming

COPY requirements.txt /opt/streaming/

RUN apt-get update \
    && apt-get install -y openjdk-11-jdk \
    && pip install --no-cache-dir -r /opt/streaming/requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

CMD ["tail" "-F" "anything"]