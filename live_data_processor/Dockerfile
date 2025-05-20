FROM ghcr.io/fiaisis/runner:281ddd7ca3cb557b48e6897412ae1283bca25e5d

WORKDIR /app

COPY ./live_data_processor/main.py main.py

RUN pip install websockets

ENTRYPOINT ["python", "main.py"]