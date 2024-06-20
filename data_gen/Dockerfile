FROM python:3.10-slim

WORKDIR /opt/datagen

COPY requirements.txt /opt/datagen/

RUN pip install --no-cache-dir -r /opt/datagen/requirements.txt

CMD ["tail" "-F" "anything"]