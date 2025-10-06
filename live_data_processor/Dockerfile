FROM ghcr.io/fiaisis/mantid:6.13.1

RUN pip install kafka-python==2.2.15
RUN pip install pyepics
RUN pip install flatbuffers==25.2.10

WORKDIR /app

COPY ./live_data_processor ./live_data_processor
COPY ./schemas live_data_processor/schemas/

WORKDIR /app/live_data_processor

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
WORKDIR /app/live_data_processor
ENTRYPOINT ["python", "main.py"]