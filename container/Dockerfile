FROM python:3.13-slim

WORKDIR /live-data

COPY . .

RUN pip install .

CMD kopf run --liveness=http://0.0.0.0:8080/healthz /live-data/live_data_operator/main.py --verbose

