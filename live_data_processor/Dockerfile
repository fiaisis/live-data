FROM ghcr.io/fiaisis/runner:281ddd7ca3cb557b48e6897412ae1283bca25e5d

WORKDIR /live_data_processor

COPY ./live_data_processor .
COPY ./schemas /schemas/

RUN pip install kafka-python==2.2.15
RUN pip install pyepics
RUN pip install flatbuffers==25.2.10


ENTRYPOINT ["python", "main.py"]