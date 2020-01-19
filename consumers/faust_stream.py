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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect-stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def stations(train_events):
    async for event in train_events:
        table[event.stop_id] = get_transformed_station(event)


if __name__ == "__main__":
    app.main()
