import requests
import math
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from source_bronnoyregister.async_get_helper import get_all

# Basic full refresh stream
class BronnoyregisterBaseUpdateStream(HttpStream, ABC):

    url_base = "https://data.brreg.no/enhetsregisteret/api/oppdateringer/"

    primary_key = "update_id"

    def __init__(self, **kwargs):
        self.start_date = kwargs.pop("start_date") + 'T00:00:00.000Z' # Adjusting format for API
        self.batch_size = kwargs.pop("batch_size")
        self.include_objects = kwargs.pop("include_objects")
        self.max_entries = kwargs.pop('max_entries', None)
        self.num_entries_so_far = 0
        self.next_id = self._get_initial_id()
        super().__init__(**kwargs)

    @abstractmethod
    def _get_initial_id(self):
        """ This function needs to be implemented in derived classes to 
        retrieve the initial update id based on the given start date. 
        
        Must set self.next_id.
        """

    @abstractmethod
    def _get_response_key_update(self) -> str:
        """ This function a keyword to access isolated objects in the 
        response. Depends on the endpoint, e.g. 'oppdaterteEnheter' for the oppdateringer/enhet endpoint

        :return Key for accessing results (String)
        """

    @abstractmethod
    def _get_response_key_entry(self) -> str:
        """ This function a keyword to access isolated objects in the 
        response. Depends on the endpoint, e.g. 'oppdaterteEnheter' for the oppdateringer/enhet endpoint

        :return Key for accessing results (String)
        """

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.json()['page']['totalElements'] == 0:
            # If response contains no more elements, no more queries are needed -> return None
            return None
        response_as_list = response.json()['_embedded'][self._get_response_key_update()]
        self.num_entries_so_far = self.num_entries_so_far + len(response_as_list)
        if response.status_code == 200 and len(response_as_list) > 0 and (self.max_entries < 0 or self.num_entries_so_far < self.max_entries):
            self.next_id = response_as_list[-1]['oppdateringsid'] + 1
            return { "next_id" : self.next_id }

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if self.next_id is None:
            # Should only be the case if there is no new entry from the after the given starting date
            return {
                "dato": self.start_date,
                "size": self.batch_size,
            }
        else:
            return {
                "oppdateringsid": self.next_id,
                "size": self.batch_size,
            }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.json()['page']['totalElements'] > 0 and self.include_objects:
            updates = response.json()['_embedded'][self._get_response_key_update()]
            # Get updated objects (using async and httpx for parallelized querying)
            urls = [update['_links'][self._get_response_key_entry()]['href'] for update in updates]
            objects = asyncio.run(get_all(urls))
            yield from (
                [
                    {
                        'update_id': json_entry['oppdateringsid'],
                        'update_timestamp' : json_entry['dato'],
                        'update_detail' : json_entry,
                        'object_detail' : objects[i]
                    }
                for i, json_entry in enumerate(updates)])
        elif response.json()['page']['totalElements'] > 0:
            updates = response.json()['_embedded'][self._get_response_key_update()]
            yield from (
                [
                    {
                        'update_id': json_entry['oppdateringsid'],
                        'update_timestamp' : json_entry['dato'],
                        'update_detail' : json_entry
                    }
                for json_entry in updates])            
        else:
            # Yield from empty list of no entries left
            yield from ([])

# Basic incremental stream
class IncrementalBronnoyregisterBaseUpdateStream(BronnoyregisterBaseUpdateStream, ABC):

    # We only persist the state if the entire stream has been read
    state_checkpoint_interval = math.inf

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def cursor_field(self) -> str:
        """
        This field should indicate when an entry has been most recently updated. 
        Can be overriden by inheriting classes.

        :return str: The name of the cursor field.
        """
        return "update_id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if len(stream_state.keys()) == 0 or next_page_token is not None:
            # This branch is entered in two cases:
            # 1) stream_state is empty, which is the case for the first run
            # 2) next_page_token is null, which is the first batch of data within each cycle.
            params = super().request_params(stream_state, stream_slice, next_page_token)
        else:
            params = {
                "oppdateringsid": stream_state['oppdateringsid'] + 1,
                "size": self.batch_size,
            }
        return params


    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {
                'oppdateringsid': latest_record['update_id']
            }