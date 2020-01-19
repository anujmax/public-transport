"""Contains functionality related to Weather"""
import logging

import json

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        if message.topic() == "org.chicago.cta.weather.v1":
            try:
                value = json.loads(message.value())
                self.temperature = value.temperature
                self.status = value.temperature
            except Exception as e:
                logger.fatal("bad weather info? %s, %s", value, e)
        else:
            logger.error(
                "unable to find handler for message from topic %s", message.topic
            )
