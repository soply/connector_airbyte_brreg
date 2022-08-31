import requests
import asyncio
from .base import BRREGUpdateStream
from typing import Any, Mapping, MutableMapping
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from source_bronnoyregister.async_get_helper import get_all

class CompanyRoles(BRREGUpdateStream):

    def _get_response_key_update(self) -> str:
        """ This function a keyword to access isolated objects in the 
        response. Depends on the endpoint, e.g. 'oppdaterteEnheter' for the oppdateringer/enhet endpoint

        :return Key for accessing results (String)
        """
        return 'oppdaterteEnheter'

    def _get_response_key_entry(self) -> str:
        return 'enhet'

    def primary_key(self) -> str:
        return "organisasjonsnummer"

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
        return "application/cloudevents-batch+json"

    def get_update_id_for_date(
        self, 
        date : str
    ) -> int:
        """_summary_

        Parameters
        ----------
        date : str
            a date in the format 'YYYY-mm-ddT00:00:00.000Z'

        Returns
        -------
        int
            Update id associated with the given date. All updates from this id onwards 
            are the same than those specified by the from date
        """
        # This retrieves sub url to updates
        suburl = self.path(stream_state = {}, next_page_token={})
        response = requests.get(self.url_base + suburl + '?afterTime=' + date)
        if len(response.json()) == 0:
            return None
        else:
            return int(response.json()[0]['id'])

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if self.is_in_initial_phase(stream_state, next_page_token):
            # Initial fetch phase
            params = super().request_params(stream_state, stream_slice, next_page_token)
        elif next_page_token is not None and 'next_id' in next_page_token.keys():
            # Update fetch phase - mid stream after restart happened
            params = {
                "afterId": next_page_token['next_id'] - 1,
                "size": self.batch_size,
            }
        else:
            # Update fetch phase - restarting from saved state or initial start date if given
            if self.start_date:
                start_update_id = max(stream_state.get('next_id', -1), self.get_update_id_for_date(self.start_date))
            else:
                start_update_id = stream_state['next_id']
            params = {
                "afterId": start_update_id - 1,
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
        if self.is_response_to_initial_load(response):
            # Initial fetch phase - retrieve next update if from given date.
            return { 'next_id' : self.get_update_id_for_date(self.fetch_updates_from_date) }
        else:
            # Update fetch phase
            if len(response.json()) == 0:
                # If response contains no more elements, no more queries are needed -> return None
                return None
            response_as_list = response.json()
            self.num_entries_so_far = self.num_entries_so_far + len(response_as_list)
            if response.status_code == 200 and len(response_as_list) > 0 and (self.max_entries < 0 or self.num_entries_so_far < self.max_entries):
                self.next_id = int(response_as_list[-1]['id']) + 1
                return { "next_id" : self.next_id }

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
            if len(response.json()) > 0:
                updates = response.json()
                # Get updated objects (using async and httpx for parallelized querying)
                urls = [update['source'] for update in updates]
                objects = asyncio.run(get_all(urls))

                yield from (
                    [
                        {
                            'update_id': json_entry['id'],
                            'update_timestamp' : json_entry['time'],
                            'update_detail' : json_entry,
                            'object_detail' : objects[i]
                        }
                    for i, json_entry in enumerate(updates)
                    ]
                )      
            else:
                # Yield from empty list of no entries left
                yield from ([])
                
    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        if self.is_in_initial_phase(stream_state, next_page_token):
            # Initial fetch phase
            return "roller/totalbestand"
        else:
            # Fetch updates phase
            return "oppdateringer/roller"