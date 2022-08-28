from .base import BRREGBatchStream
from typing import Any, Mapping
import requests

class Company(BRREGBatchStream):

    # def _get_initial_id(self):
    #     """ This function needs to be implemented in derived classes to 
    #     retrieve the initial update id based on the given start date. 
        
    #     Must set self.next_id.
    #     """
    #     response = requests.get(self.url_base + 'enheter?dato=' + self.start_date)
    #     if response.json()['page']['totalElements'] == 0:
    #         return None
    #     response_as_list = response.json()['_embedded'][self._get_response_key_update()]
    #     return response_as_list[0]['oppdateringsid']

    # def _get_response_key_update(self) -> str:
    #     """ This function a keyword to access isolated objects in the 
    #     response. Depends on the endpoint, e.g. 'oppdaterteEnheter' for the oppdateringer/enhet endpoint

    #     :return Key for accessing results (String)
    #     """
    #     return 'oppdaterteEnheter'

    # def _get_response_key_entry(self) -> str:
    #     return 'enhet'

    def primary_key(self):
        return "organisasjonsnummer"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "enheter/lastned"

    def _header_accept(self):
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