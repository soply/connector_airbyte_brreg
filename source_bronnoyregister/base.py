import requests
import math
import asyncio
import datetime
from itertools import islice
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from source_bronnoyregister.async_get_helper import get_all
from source_bronnoyregister.brreg_batch_stream_decoder import BRREGBatchStreamDecoder


# Basic full refresh stream
class BRREGBatchStream(HttpStream, ABC):
    """
    Creates a BRREG batch stream that streams a complete file 
    (batch download) to the destination.
    """

    url_base = "https://data.brreg.no/enhetsregisteret/api/"

    def __init__(self, **kwargs):
        # Batch size is not applicable for the batch stream, since the data is streamed.
        # We still have to pop it not to confused super constructor.
        kwargs.pop("batch_size", None)
        self.max_entries = kwargs.pop('max_entries')
        if self.max_entries is None:
            # If parameter is not specified in spec we fetch all data
            self.max_entries = -1
        self.num_entries_so_far = 0
        super().__init__(**kwargs)

    def process_object(self, obj : Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Processes the objects to give them the same shape as update data

        Parameters
        ----------
        obj : Mapping[str, Any]
            Original object

        Returns
        -------
        Mapping[str, Any]
            Processed object
        """
        return {
            "update_id" : None,
            "update_timestamp" : None,
            "update_detail" : None,
            "object_detail" : obj
        }


    @abstractmethod
    def _header_accept(self) -> str:
        """
        Returns the value used for "Accept" keyword in headers 
        when sending requests. E.g. used to specify the version 
        of the returned objects.

        Returns
        -------
        str
            Value of "Accept" header parameter
        """

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """ 
        Airbyte doc: Override this method to define a pagination strategy.
        The value returned from this method is passed to most other methods in this class. Use it to form a request e.g: set headers or query params.
        :return: The token for the next page from the input response object. Returning None means there are no more pages to read in this response.
        
        Custom doc: Returns an empty dict because no pagination is used during the batch download.

        Returns
        -------
        dict
            Empty dict
        """
        return {}

    def request_headers(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Airbyte doc: Override to return any non-auth headers. Authentication headers will overwrite any overlapping headers returned from this method.

        Custom doc: Specifying the version of the retrieved objects by using the Accept header.

        Returns
        -------
        dict
            Dict with headers passed to the request
        """
        return {
            "Accept" : self._header_accept(),
        }


    def request_kwargs(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Airbyte doc: Override to return a mapping of keyword arguments to be used when creating the HTTP request.
        Any option listed in https://docs.python-requests.org/en/latest/api/#requests.adapters.BaseAdapter.send for can be returned from
        this method. Note that these options do not conflict with request-level options such as headers, request params, etc..

        Custom doc: Ensures the request response is streamed

        Returns
        -------
        dict
            Dict ensuring the requests response is streamed
        """
        return {
            "stream" : True
        }

    def parse_response(self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        """
        Parses the raw response object into a list of records.
        By default, this returns an iterable containing the input. Override to parse differently.
        :param response:
        :return: An iterable containing the parsed response
        """
        bbsd = BRREGBatchStreamDecoder(response, self.process_object)
        if self.max_entries > 0:
            yield from islice(bbsd, self.max_entries)
        else:
            yield from bbsd


# Basic incremental stream
class BRREGUpdateStream(BRREGBatchStream, ABC):

    # We only persist the state if the entire stream has been read
    state_checkpoint_interval = math.inf

    def __init__(self, **kwargs):
        # To make sure we receive all updates, we fetch the latest update ids
        # from 2 days ago. This means, after initial fetch, we fetch all 
        # updates starting with those from 2 days ago. Ensures to receive all data
        today = datetime.date.today() - datetime.timedelta(days=2)
        # Datetime in BRREG format
        self.fetch_updates_from_date = today.strftime('%Y-%m-%d') + 'T00:00:00.000Z'
        self.batch_size = kwargs.pop("batch_size")
        super().__init__(**kwargs)

    @property
    def cursor_field(self) -> str:
        """
        This field should indicate when an entry has been most recently updated. 
        Can be overriden by inheriting classes.

        :return str: The name of the cursor field.
        """
        return "update_id"

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

    def request_headers(
        self,
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Airbyte doc: Override to return any non-auth headers. Authentication headers will overwrite any overlapping headers returned from this method.

        Custom doc: 

        Returns
        -------
        dict
            
        """
        if len(stream_state.keys()) == 0 and next_page_token is None:
            # Initial fetch phase
            return super().request_headers(stream_state, stream_slice, next_page_token)
        else:
            # Update fetch phase
            return {}

    def request_kwargs(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Airbyte doc: Override to return a mapping of keyword arguments to be used when creating the HTTP request.
        Any option listed in https://docs.python-requests.org/en/latest/api/#requests.adapters.BaseAdapter.send for can be returned from
        this method. Note that these options do not conflict with request-level options such as headers, request params, etc..

        Custom doc: 
z
        Returns
        -------
        dict
            
        """
        if len(stream_state.keys()) == 0 and next_page_token is None:
            # Initial fetch phase
            return super().request_kwargs(stream_state, stream_slice, next_page_token)
        else:
            # Update fetch phase
            return {}

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if len(stream_state.keys()) == 0 and next_page_token is None:
            # Initial fetch phase
            params = super().request_params(stream_state, stream_slice, next_page_token)
        elif next_page_token is not None and 'fetch_updates_from_date' in next_page_token.keys():
            # Transition from intial to update fetch phase
            params = {
                "dato": next_page_token['fetch_updates_from_date'],
                "size": self.batch_size,
            }
        elif next_page_token is not None and 'next_id' in next_page_token.keys():
            # Update fetch phase - mid stream after restart happened
            params = {
                "oppdateringsid": next_page_token['next_id'],
                "size": self.batch_size,
            }
        else:
            # Update fetch phase - restarting from saved state
            params = {
                "oppdateringsid": stream_state['next_id'],
                "size": self.batch_size,
            }
        return params

    def next_page_token(self, 
        response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """ 
        Airbyte doc: Override this method to define a pagination strategy.
        The value returned from this method is passed to most other methods in this class. Use it to form a request e.g: set headers or query params.
        :return: The token for the next page from the input response object. Returning None means there are no more pages to read in this response.
        
        Custom doc: 

        Returns
        -------
        dict
        """
        if response.request.url.endswith(self.path(stream_state={})):
            # Initial fetch phase
            return {
                'fetch_updates_from_date' : self.fetch_updates_from_date
            }
        else:
            # Update fetch phase
            if response.json()['page']['totalElements'] == 0:
                # If response contains no more elements, no more queries are needed -> return None
                return None
            response_as_list = response.json()['_embedded'][self._get_response_key_update()]
            self.num_entries_so_far = self.num_entries_so_far + len(response_as_list)
            if response.status_code == 200 and len(response_as_list) > 0 and (self.max_entries < 0 or self.num_entries_so_far < self.max_entries):
                self.next_id = response_as_list[-1]['oppdateringsid'] + 1
                return { "next_id" : self.next_id }

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        if latest_record.get('update_id', None):
            # Update fetch phase
            return {
                    'next_id': latest_record['update_id'] + 1
                }
        else:
            # Initial fetch phase                
            return {}

    def parse_response(self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        if response.request.url.endswith(self.path(stream_state={})):
            # Initial fetch phase
            yield from super().parse_response(response, stream_state, stream_slice, next_page_token)
        else:
            # Update fetch phase
            if response.json()['page']['totalElements'] > 0:
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
                    for i, json_entry in enumerate(updates)
                    ]
                )      
            else:
                # Yield from empty list of no entries left
                yield from ([])