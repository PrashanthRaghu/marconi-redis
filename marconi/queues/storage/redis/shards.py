# Copyright (c) 2014 Prashanth Raghu.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""shards: an implementation of the shard management storage
controller for redis.

   Redis Data Structures:
   ---------------------
   Shards ( Redis Set )
   --------------------
   Contains the list of all shards.
   id: shards
         Id                   Value
        -----------------------------
        'name'   ->        shard_name

    Shard Info ( Redis Hash )
    -------------------------
    id : Shard Name
    Schema:
       Id                   Value
        ---------------------------------
        'u'      ->         uri
        'w'      ->         weight
        'o'      ->         options
"""

from marconi.queues.storage import base
from marconi.queues import storage
from marconi.queues.storage.redis import utils

class ShardsController(base.ShardsBase):

    def __init__(self, *args, **kwargs):
        super(ShardsController, self).__init__(*args, **kwargs)
        self._driver = self.driver.connection

    @utils.raises_conn_error
    def list(self, marker=None, limit=storage.DEFAULT_SHARDS_PER_PAGE,
             detailed=False):
        raise NotImplementedError

    @utils.raises_conn_error
    def create(self, name, weight, uri, options=None):
        raise NotImplementedError

    @utils.raises_conn_error
    def get(self, name, detailed=False):
        raise NotImplementedError

    @utils.raises_conn_error
    def exists(self, name):
        raise NotImplementedError

    @utils.raises_conn_error
    def delete(self, name):
        raise NotImplementedError

    @utils.raises_conn_error
    def update(self, name, **kwargs):
        raise NotImplementedError

    @utils.raises_conn_error
    def drop_all(self):
        raise NotImplementedError

