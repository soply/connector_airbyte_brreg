

# # Basic full refresh stream
# class BronnoyregisterBaseUpdateStream(HttpStream, ABC):

#     def __init__(self, **kwargs):
#         # To make sure we receive all updates, we fetch the latest update ids
#         # from 2 days ago. This means, after initial fetch, we fetch all 
#         # updates starting with those from 2 days ago. Ensures to receive all data
#         today = datetime.date.today() - datetime.timedelta(days=2)
#         # Datetime in BRREG format
#         self.fetch_updates_from_date = today.strftime('%Y-%m-%d') + 'T00:00:00.000Z'
#         self.batch_size = kwargs.pop("batch_size")
#         self.max_entries = kwargs.pop('max_entries')
#         if self.max_entries is None:
#             # If parameter is not specified in spec we fetch all data
#             self.max_entries = -1
#         self.num_entries_so_far = 0
#         self.start_updates_from_id = self._get_initial_id()
#         super().__init__(**kwargs)

#     def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
#         if response.json()['page']['totalElements'] == 0:
#             # If response contains no more elements, no more queries are needed -> return None
#             return None
#         response_as_list = response.json()['_embedded'][self._get_response_key_update()]
#         self.num_entries_so_far = self.num_entries_so_far + len(response_as_list)
#         if response.status_code == 200 and len(response_as_list) > 0 and (self.max_entries < 0 or self.num_entries_so_far < self.max_entries):
#             self.next_id = response_as_list[-1]['oppdateringsid'] + 1
#             return { "next_id" : self.next_id }
    
#     def request_params(
#         self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> MutableMapping[str, Any]:
#         if self.next_id is None:
#             # Should only be the case if there is no new entry from the after the given starting date
#             return {
#                 "dato": self.start_date,
#                 "size": self.batch_size,
#             }
#         else:
#             return {
#                 "oppdateringsid": self.next_id,
#                 "size": self.batch_size,
#             }

#     def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
#         if response.json()['page']['totalElements'] > 0 and self.include_objects:
#             updates = response.json()['_embedded'][self._get_response_key_update()]
#             # Get updated objects (using async and httpx for parallelized querying)
#             urls = [update['_links'][self._get_response_key_entry()]['href'] for update in updates]
#             objects = asyncio.run(get_all(urls))
#             yield from (
#                 [
#                     {
#                         'update_id': json_entry['oppdateringsid'],
#                         'update_timestamp' : json_entry['dato'],
#                         'update_detail' : json_entry,
#                         'object_detail' : objects[i]
#                     }
#                 for i, json_entry in enumerate(updates)])
#         elif response.json()['page']['totalElements'] > 0:
#             updates = response.json()['_embedded'][self._get_response_key_update()]
#             yield from (
#                 [
#                     {
#                         'update_id': json_entry['oppdateringsid'],
#                         'update_timestamp' : json_entry['dato'],
#                         'update_detail' : json_entry
#                     }
#                 for json_entry in updates])            
#         else:
#             # Yield from empty list of no entries left
#             yield from ([])