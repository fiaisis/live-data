FROM python:3.14-slim

WORKDIR /live_data_operator

COPY ./live_data_operator .

RUN pip install --root-user-action ignore .

CMD kopf run --liveness=http://0.0.0.0:8080/healthz main.py --verbose

