# BSD 2-Clause License
#
# Copyright (c) 2021, European Spallation Source - Data Management and Software Centre
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This code is adapted from https://github.com/ess-dmsc/nexus-streamer-python

from collections.abc import Callable, Generator
from contextlib import suppress
from datetime import UTC

import dateutil.parser
import h5py
import numpy as np
from pint import UndefinedUnitError, UnitRegistry

ureg = UnitRegistry()
SECONDS = ureg("seconds")
MILLISECONDS = ureg("milliseconds")
MICROSECONDS = ureg("microseconds")
NANOSECONDS = ureg("nanoseconds")


def seconds_to_nanoseconds(input_value: float | int | np.ndarray) -> np.ndarray:
    return (np.array(input_value) * 1_000_000_000).astype(int)


def milliseconds_to_nanoseconds(input_value: float | int | np.ndarray) -> np.ndarray:
    return (np.array(input_value) * 1_000_000).astype(int)


def microseconds_to_nanoseconds(input_value: float | int | np.ndarray) -> np.ndarray:
    return (np.array(input_value) * 1_000).astype(int)


def nanoseconds_to_nanoseconds(input_value: float | int | np.ndarray) -> np.ndarray:
    return np.array(input_value).astype(int)


def get_to_nanoseconds_conversion_method(units: str | bytes) -> Callable:
    with suppress(TypeError):
        units = str(units, encoding="utf8")  # type: ignore
    input_units = ureg(units)

    if input_units == SECONDS:
        return seconds_to_nanoseconds
    if input_units == MILLISECONDS:
        return milliseconds_to_nanoseconds
    if input_units == MICROSECONDS:
        return microseconds_to_nanoseconds
    if input_units == NANOSECONDS:
        return nanoseconds_to_nanoseconds
    raise UndefinedUnitError


def iso8601_to_ns_since_epoch(iso8601_timestamp: str | bytes) -> int:
    with suppress(TypeError):
        iso8601_timestamp = str(iso8601_timestamp, encoding="utf8")  # type: ignore
    offset_datetime = dateutil.parser.parse(iso8601_timestamp)
    if offset_datetime.tzinfo is None:
        # Assume time is UTC if it was not explicit
        offset_datetime = offset_datetime.replace(tzinfo=UTC)
    return int(offset_datetime.timestamp() * 1_000_000_000)  # s float to ns int


def _get_pulse_time_offset_in_ns(pulse_time_dataset: h5py.Group) -> int:
    """
    Gives an offset which, when added to pulse times, results in time relative to unix epoch
    """
    try:
        date_string = pulse_time_dataset.attrs["offset"]
    except KeyError:
        # If no "offset" attribute then times are already relative to unix epoch according to NeXus standard
        return 0
    return iso8601_to_ns_since_epoch(date_string)


def _get_pulse_time_unit_converter(group: h5py.Group) -> Callable:
    try:
        units = group["event_time_zero"].attrs["units"]
    except AttributeError as e:
        raise UndefinedUnitError from e
    return get_to_nanoseconds_conversion_method(units)


class _ChunkDataLoader:
    def __init__(self, dataset: h5py.Dataset):
        self._dataset = dataset
        self._chunk_iterator = self._dataset.iter_chunks()
        next_slice = next(self._chunk_iterator)
        self._current_chunk = self._dataset[next_slice]
        self._start_index: int = 0

    def get_data_for_pulse(self, pulse_start_event: int, pulse_end_event: int) -> np.ndarray:
        start_index = int(pulse_start_event - self._start_index)
        end_index = int(pulse_end_event - self._start_index)

        data_for_pulse = np.array([], dtype=self._current_chunk.dtype)
        while True:
            # If all the data we need is in the current, cached chunk,
            # then just append and return it
            if end_index < self._current_chunk.size:
                return np.append(data_for_pulse, self._current_chunk[start_index:end_index])
            # else...
            # we need all the data in the current chunk...
            data_for_pulse = np.append(data_for_pulse, self._current_chunk[start_index:])
            # and at least some from the next chunk, so load the next chunk and continue
            end_index -= self._current_chunk.size
            start_index = 0
            self._start_index += self._current_chunk.size
            try:
                next_slice = next(self._chunk_iterator)
            except StopIteration:
                return data_for_pulse
            self._current_chunk = self._dataset[next_slice]


class _ContiguousDataLoader:
    def __init__(self, dataset: h5py.Dataset):
        self._dataset = dataset
        max_bytes_willing_to_load_into_memory = 100_000_000  # 100 MB
        if self._dataset.nbytes < max_bytes_willing_to_load_into_memory:
            self._dataset = self._dataset[...]
        elif self._dataset.compression is not None:
            print(  # noqa: T201
                f"{self._dataset.name} is larger than {max_bytes_willing_to_load_into_memory} bytes,"
                f"contiguous and compressed, it will be very slow to stream if these event data are from many pulses"
            )

    def get_data_for_pulse(self, pulse_start_event: int, pulse_end_event: int) -> np.ndarray:
        return self._dataset[pulse_start_event:pulse_end_event]


_DataLoader = _ChunkDataLoader | _ContiguousDataLoader


class EventDataSource:
    def __init__(self, group: h5py.Group):
        """
        Load data, one pulse at a time from NXevent_data in NeXus file
        :raises BadSource if there is a critical problem with the data source
        """
        self._group = group

        self._tof_loader: _DataLoader
        self._id_loader: _DataLoader

        if self._has_missing_fields():
            raise RuntimeError()
        try:
            self._convert_pulse_time = _get_pulse_time_unit_converter(group)
            self._convert_event_time = self._get_event_time_unit_converter()
        except UndefinedUnitError as e:
            print(  # noqa: T201
                f"Unable to publish data from NXevent_data at {self._group.name} due to unrecognised "
                f"or missing units for time field"
            )
            raise RuntimeError() from e

        self._event_time_zero = self._group["event_time_zero"][...]
        self._event_index = self._group["event_index"][...]

        # There is some variation in the last recorded event_index in files from different institutions
        # for example ISIS files often have what would be the first index of the next pulse at the end.
        # This logic hopefully covers most cases
        if self._event_index[-1] < self._group["event_id"].len():
            self._event_index = np.append(
                self._event_index,
                np.array([self._group["event_id"].len() - 1]).astype(self._event_index.dtype),
            )
        else:
            self._event_index[-1] = self._group["event_id"].len()

        try:
            self._group["event_time_offset"].iter_chunks()
            self._tof_loader = _ChunkDataLoader(self._group["event_time_offset"])
        except TypeError:
            self._tof_loader = _ContiguousDataLoader(self._group["event_time_offset"])

        try:
            self._group["event_id"].iter_chunks()
            self._id_loader = _ChunkDataLoader(self._group["event_id"])
        except TypeError:
            self._id_loader = _ContiguousDataLoader(self._group["event_id"])

        self._pulse_time_offset_ns = _get_pulse_time_offset_in_ns(self._group["event_time_zero"])

    @property
    def final_timestamp(self) -> int:
        # Last pulse time is good enough, we won't try to find the last event in the last pulse
        return self._convert_pulse_time(self._event_time_zero[-1]) + self._pulse_time_offset_ns

    def get_data(
        self,
    ) -> Generator[tuple[np.ndarray | None, np.ndarray | None, int], None, None]:
        """
        Returns None instead of a data when there is no more data
        """
        # -1 as last index would be start of the next pulse after the end of the run
        for pulse_number in range(self._event_index.size - 1):
            pulse_time = self._convert_pulse_time(self._event_time_zero[pulse_number]) + self._pulse_time_offset_ns
            start_event = self._event_index[pulse_number]
            end_event = self._event_index[pulse_number + 1]
            yield (
                self._convert_event_time(self._tof_loader.get_data_for_pulse(start_event, end_event)),
                self._id_loader.get_data_for_pulse(start_event, end_event),
                int(pulse_time),
            )
        yield None, None, 0

    def _has_missing_fields(self) -> bool:
        missing_field = False
        required_fields = (
            "event_time_zero",
            "event_index",
            "event_id",
            "event_time_offset",
        )
        for field in required_fields:
            if field not in self._group:
                print(  # noqa: T201
                    f"Unable to publish data from NXevent_data at {self._group.name} due to missing {field} field"
                )
                missing_field = True
        return missing_field

    def _get_event_time_unit_converter(self) -> Callable:
        try:
            units = self._group["event_time_offset"].attrs["units"]
        except AttributeError as err:
            raise UndefinedUnitError from err
        return get_to_nanoseconds_conversion_method(units)

    @property
    def name(self):
        return self._group.name.split("/")[-1]
