"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


def get_line(station):
    if station.red:
        line = "red"
    else:
        if station.blue:
            line = "blue"
        else:
            line = "green"
    return line


def get_transformed_station(station):
    return TransformedStation(
        station_id=station.stop_id,
        station_name=station.stop_name,
        order=station.order,
        line=get_line(station)
    )


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("connect-stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("faust.transformed-stations", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "transformed_stations",  # "TODO",
    default=int,  # default=TODO,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def stations(trainevents):
    async for event in trainevents:
        table[event.stop_id] = get_transformed_station(event)


if __name__ == "__main__":
    app.main()
