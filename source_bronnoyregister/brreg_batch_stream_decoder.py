import requests
import zlib
import re 
import json


class BRREGBatchStreamDecoder():

        def __init__(self, response: requests.Response):
            self._response = response
            self._byte_stream = self._response_byte_stream()
            self._current_string = None
            self._current_position = 0


        def __iter__(self):
            return self

        def _response_byte_stream(self):
            dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
            for i, obj in enumerate(self._response):
                rv = dec.decompress(obj)
                yield rv

        def _read_next_from_stream(self):
            can_be_decoded = False
            decoded_resp = ''
            undecoded_resp = b''
            while not can_be_decoded:
                next_response = next(self._byte_stream)
                undecoded_resp += next_response
                try:
                    decoded_resp += undecoded_resp.decode('utf8')
                    can_be_decoded = True
                except:
                    pass
            return decoded_resp

        def __next__(self):
            while True:
                if self._current_string is None:
                    self._current_string = self._read_next_from_stream()
                    self._current_string = self._current_string.replace('\n', '')
                while True:
                    self._current_string = re.sub(r'^.*?{', '{', self._current_string)
                    next_position_to_check_offset = self._current_string[self._current_position:].find('}')
                    next_position_to_check = self._current_position + next_position_to_check_offset
                    while next_position_to_check_offset != -1:
                        self.position = next_position_to_check + 1
                        try:
                            test = json.loads(self._current_string[:next_position_to_check+1])
                            self._current_string = self._current_string[next_position_to_check + 2:]
                            self._current_position = 0
                            return test
                        except ValueError:
                            pass
                        next_position_to_check_offset = self._current_string[self.position:].find('}')
                        next_position_to_check = self.position + next_position_to_check_offset
                    self._current_string += self._read_next_from_stream()
                    self._current_string = self._current_string.replace('\n', '')