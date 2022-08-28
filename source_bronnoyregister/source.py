import requests

from typing import Any, List, Mapping, Tuple
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .company import Company


# Source
class SourceBronnoyregister(AbstractSource):

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        response = requests.get("https://data.brreg.no/enhetsregisteret/api")
        if response.status_code == 200:
            return True, None
        else:
            return False, response.raise_for_status()

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # Get a token from microservice
        start_date = config.get('start_date', None)
        if config["batch_size"] > 10000:
            raise RuntimeError('Batch size exceeding 10 000 is not allowed - please chose batch size below.')
        return [
                    Company(
                        batch_size=config["batch_size"], 
                        max_entries = config.get("max_entries")
                    ),
                ]
