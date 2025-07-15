import logging
import time

import flatbuffers
import h5py
import numpy as np
from compiled_schemas.EventMessage import (
    AddDetectorId,
    AddMessageId,
    AddPulseTime,
    AddSourceName,
    AddTimeOfFlight,
    End,
    Start,
    StartDetectorIdVector,
    StartTimeOfFlightVector,
)
from kafka import KafkaProducer
from nexus_loading import EventDataSource

logging.basicConfig(level=logging.DEBUG)


def build_ev42_from_dict(ev42_message: dict) -> bytes:
    builder = flatbuffers.Builder(initialSize=1024)

    # Convert lists to numpy arrays to ensure uint32 typing
    tof_array = np.array(ev42_message["event_time_offset"], dtype=np.uint32)
    det_array = np.array(ev42_message["event_id"], dtype=np.uint32)

    # Build time_of_flight vector
    StartTimeOfFlightVector(builder, len(tof_array))
    for t in reversed(tof_array):
        builder.PrependUint32(t)
    tof_vec = builder.EndVector()

    # Build detector_id vector
    StartDetectorIdVector(builder, len(det_array))
    for d in reversed(det_array):
        builder.PrependUint32(d)
    det_vec = builder.EndVector()

    # Source name
    source_name_offset = builder.CreateString(ev42_message.get("source_name", "simulator"))

    # Build the EventMessage
    Start(builder)
    AddSourceName(builder, source_name_offset)
    AddMessageId(builder, ev42_message["message_id"])
    AddPulseTime(builder, ev42_message["pulse_time"])
    AddTimeOfFlight(builder, tof_vec)
    AddDetectorId(builder, det_vec)
    msg = End(builder)

    # Finalize buffer with identifier "ev42"
    builder.Finish(msg, file_identifier=b"ev42")

    return bytes(builder.Output())


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: v,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username="superuser",
        sasl_plain_password="secretpassword",
    )
    with h5py.File("MER72596.nxs", "r") as f:
        group = f["/raw_data_1/detector_1_events"]
        print("before source")
        source = EventDataSource(group)
        print("after source")
        wall_start = time.time()
        data_start = None

        start_us = source._convert_pulse_time(source._event_time_zero[0])
        end_us = source._convert_pulse_time(source._event_time_zero[-1])
        duration_sec = (end_us - start_us) * 1e-9
        print(f"Total simulated duration: {duration_sec / 60:.2f} minutes")

        for index, (tofs, ids, pulse_time) in enumerate(source.get_data()):
            if tofs is None:
                break  # end of data
            if data_start is None:
                data_start = pulse_time
                print(f"Simulation starts at pulse_time = {data_start}")
            rel_time = (pulse_time - data_start) * 1e-9
            now = time.time() - wall_start
            sleep = rel_time - now
            if sleep > 0:
                time.sleep(sleep)

            MAX_EVENTS = 10
            event_time_offset = tofs[:MAX_EVENTS].tolist()
            event_id = ids[:MAX_EVENTS].tolist()
            ev42_message = {
                "message_type": "ev42",
                "message_id": index,
                "pulse_time": pulse_time,
                "event_time_offset": tofs.tolist(),
                "event_id": ids.tolist(),
                # "event_time_offset": event_time_offset,
                # "event_id": event_id,
            }

            producer.send("fake_merlin", (build_ev42_from_dict(ev42_message)))
            producer.flush()


main()
