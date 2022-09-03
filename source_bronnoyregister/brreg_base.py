import requests
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class BRREGBatchBaseStream(HttpStream, ABC):
    """
    Abstract class for brreg batch streames. There are two different types
    of batch streams with the brreg resource:
    
    1) Endpoints with few resources / items, where the batch result is returned as a json, and the objects
    can be yielded from a list without additional work (organization_types, kommuner, 
    rolletyper, rollegruppetyper, representanter)

    2) Endpoints with many resources / items, where the data is zipped before receiving it. In this case
    we stream the data piece by piece, to avoid high memory constraints, and decode on the fly (enhet, 
    underenhet, roller).
    """

    url_base = "https://data.brreg.no/enhetsregisteret/api/"

    def __init__(self, **kwargs):
        self.max_entries = kwargs.pop('max_entries')
        if self.max_entries is None:
            # If parameter is not specified in spec we fetch all data
            self.max_entries = -1
        self.num_entries_so_far = 0
        super().__init__(**kwargs)

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

    @abstractmethod
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
        Airbyte doc: Override to return any non-auth headers. Authentication headers will 
        overwrite any overlapping headers returned from this method.

        Custom doc: Specifying the version of the retrieved objects by using the Accept header.

        Returns
        -------
        dict
            Dict with headers passed to the request
        """
        return {
            "Accept" : self._header_accept(),
        }
