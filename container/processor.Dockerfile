FROM ghcr.io/fiaisis/mantid:6.15.0

WORKDIR /app
COPY ./live_data_processor/pyproject.toml .
COPY ./live_data_processor ./live_data_processor
RUN pip install --root-user-action ignore .

WORKDIR /app/live_data_processor

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
WORKDIR /app/live_data_processor
ENTRYPOINT ["python", "main.py"]