from .brreg_base import BRREGBatchBaseStream
from typing import Any, Iterable, Mapping
import requests

class Municipality(BRREGBatchBaseStream):

    def primary_key(self) -> str:
        return "nummer"

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
        return "application/vnd.brreg.enhetsregisteret.kommune.v1+json"

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "kommuner"

    def parse_response(self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        """
        Overwrite base method since the response type of this resource 
        requires no decoding / streaming.
        """
        yield from response.json()['_embedded']['kommuner']
