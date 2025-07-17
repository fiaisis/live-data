import datetime
import logging
import time

import flatbuffers
import h5py
import numpy as np
from compiled_schemas import EventMessage, RunStart, RunStop
from kafka import KafkaProducer
from nexus_loading import EventDataSource

logging.basicConfig(level=logging.DEBUG)


def build_ev42_from_dict(ev42_message: dict) -> bytes:
    builder = flatbuffers.Builder(initialSize=1024)

    # Convert lists to numpy arrays to ensure uint32 typing
    tof_array = np.array(ev42_message["event_time_offset"], dtype=np.uint32)
    det_array = np.array(ev42_message["event_id"], dtype=np.uint32)

    # Build time_of_flight vector
    EventMessage.StartTimeOfFlightVector(builder, len(tof_array))
    for t in reversed(tof_array):
        builder.PrependUint32(t)
    tof_vec = builder.EndVector()

    # Build detector_id vector
    EventMessage.StartDetectorIdVector(builder, len(det_array))
    for d in reversed(det_array):
        builder.PrependUint32(d)
    det_vec = builder.EndVector()

    # Source name
    source_name_offset = builder.CreateString(ev42_message.get("source_name", "simulator"))

    # Build the EventMessage
    EventMessage.Start(builder)
    EventMessage.AddSourceName(builder, source_name_offset)
    EventMessage.AddMessageId(builder, ev42_message["message_id"])
    EventMessage.AddPulseTime(builder, ev42_message["pulse_time"])
    EventMessage.AddTimeOfFlight(builder, tof_vec)
    EventMessage.AddDetectorId(builder, det_vec)
    msg = EventMessage.End(builder)

    # Finalize buffer with identifier "ev42"
    builder.Finish(msg, file_identifier=b"ev42")

    return bytes(builder.Output())


def build_run_stop(stop_time, run_name):
    builder = flatbuffers.Builder(initialSize=1024)
    # Create string first before starting the message
    run_name_offset = builder.CreateString(run_name)
    RunStop.Start(builder)
    RunStop.AddStopTime(builder, stop_time)
    RunStop.AddRunName(builder, run_name_offset)
    msg = RunStop.End(builder)
    builder.Finish(msg, file_identifier=b"pl72")
    return bytes(builder.Output())


def build_run_start(start_time, run_name, instrument_name):
    builder = flatbuffers.Builder(initialSize=1024)
    # Create strings first before starting the message
    run_name_offset = builder.CreateString(run_name)
    instrument_name_offset = builder.CreateString(instrument_name)
    RunStart.Start(builder)
    RunStart.AddStartTime(builder, start_time)
    RunStart.AddRunName(builder, run_name_offset)
    RunStart.AddInstrumentName(builder, instrument_name_offset)
    msg = RunStart.End(builder)
    builder.Finish(msg, file_identifier=b"6s4t")
    return bytes(builder.Output())


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: v,
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
        instrument = f["/raw_data_1/instrument_name"][0].decode("utf-8").strip() if "/raw_data_1/instrument_name" in f else "MERLIN"
        run_title = f["/raw_data_1/title"][0].decode("utf-8").strip()
        run_name = run_title if run_title else f"MER72596_{int(time.time())}"
        start_time = int(datetime.datetime.fromisoformat(f["/raw_data_1/start_time"][0].decode("utf-8")).timestamp())
        end_time = int(datetime.datetime.fromisoformat(f["/raw_data_1/end_time"][0].decode("utf-8")).timestamp())

        # Send RunStop for any previous run first, then submit start
        producer.send(f"{instrument}_runInfo", build_run_stop(stop_time=start_time, run_name=f"{instrument}_previous_run"))        
        producer.send(f"{instrument}_runInfo", build_run_start(start_time=start_time, run_name=run_name, instrument_name=instrument))
        producer.flush()

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
            ev42_message = {
                "message_type": "ev42",
                "message_id": index,
                "pulse_time": pulse_time,
                "event_time_offset": tofs.tolist(),
                "event_id": ids.tolist(),
                # "event_time_offset": event_time_offset,
                # "event_id": event_id,
            }

            producer.send("MERLIN_events", (build_ev42_from_dict(ev42_message)))
            producer.flush()
        producer.send(f"{instrument}_runInfo", build_run_stop(stop_time=end_time,
                                                              run_name="Some previous run"))
        producer.flush()

main()
