FROM ghcr.io/fiaisis/mantid:6.15.0

WORKDIR /app
COPY ./live_data_processor/pyproject.toml .
COPY ./live_data_processor ./live_data_processor
COPY ./live_data_processor/epics_streamer.py /epics_streamer.py
COPY ./live_data_processor/run_monitor.py /run_monitor.py
RUN pip install --root-user-action ignore .

WORKDIR /app/live_data_processor

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
CMD ["python", "main.py"]