from .base import BRREGUpdateStream
from typing import Any, Mapping
import requests


class Company(BRREGUpdateStream):

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
        return "application/vnd.brreg.enhetsregisteret.enhet.v1+gzip;charset=UTF-8"

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        if len(stream_state) == 0 and next_page_token is None:
            # Initial fetch phase
            return "enheter/lastned"
        else:
            # Fetch updates phase
            return "oppdateringer/enheter"