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
   Shards ( Redis Sorted Set )
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
from marconi.queues.storage import errors

# Datastructure id for the list of shards
SHARDS_SET_NAME = "shards"

class ShardsController(base.ShardsBase):

    def __init__(self, *args, **kwargs):
        super(ShardsController, self).__init__(*args, **kwargs)
        self._client = self.driver.connection
        self._pipeline = self._client.pipeline()

    @utils.raises_conn_error
    def list(self, marker=None, limit=storage.DEFAULT_SHARDS_PER_PAGE,
             detailed=False):
        client = self._client

        start = client.zrank(SHARDS_SET_NAME, marker) or 0
        shard_cursor = (q for q in client.zrange(SHARDS_SET_NAME, start, start+limit))

        def _it():
            for shard_name in shard_cursor:
                shard_info = client.hgetall(shard_name)
                shard_info['n'] = shard_name
                if not detailed:
                    del shard_info['o']
                yield _normalize(shard_info, detailed)

        return _it()

    @utils.raises_conn_error
    @utils.reset_pipeline
    def create(self, name, weight, uri, options=None):
        # (Doubt):The WSGI layer does not expect any
        # responsefrom the storage backend. Errors
        # are not being trapped as well.
        pipe = self._pipeline

        # NOTE(prashanthr_):Take care of this error
        # from the transport driver.
        if self.exists(name):
            raise errors.ShardAlreadyExists(name)

        shard_info = {
            'u': uri,
            'w': weight,
            'o': options
        }

        pipe.zadd(SHARDS_SET_NAME, 1, name)
        pipe.hmset(name, shard_info)
        pipe.execute()

    @utils.raises_conn_error
    def get(self, name, detailed=False):
        print "name",name
        if not self.exists(name):
            raise errors.ShardDoesNotExist(name)

        shard_info = self._client.hgetall(name)
        shard_info['n'] = name

        if not detailed:
            del shard_info['o']
        return _normalize(shard_info, detailed)

    @utils.raises_conn_error
    def exists(self, name):
        return self._client.zrank(SHARDS_SET_NAME, name) is not None

    @utils.raises_conn_error
    @utils.reset_pipeline
    def delete(self, name):
        # Note: Does not raise any errors when the shard does not
        # exist.
        pipe = self._pipeline
        pipe.delete(name)
        pipe.zrem(SHARDS_SET_NAME, name)
        pipe.execute()

    @utils.raises_conn_error
    def update(self, name, **kwargs):
        client = self._client
        if not self.exists(name):
            raise errors.ShardDoesNotExist(name)

        shard_info = client.hgetall(name)

        # Override the parameters from the client.
        shard_new_info = {
            'u': kwargs['uri'] or shard_info['u'],
            'w': kwargs['weight'] or shard_info['w'],
            'o': kwargs['options'] or shard_info['o']
        }

        client.hmset(name, shard_new_info)

    @utils.raises_conn_error
    @utils.reset_pipeline
    def drop_all(self):
        # Note(prashanthr_): Not being used from the transport
        # driver.
        pipe = self._pipeline
        shards = self._client.zrange(SHARDS_SET_NAME, 0, -1)

        # Remove all shards from the set of shards.
        pipe.zremrangebyscore(SHARDS_SET_NAME, 0, -1)
        for shard in shards:
            pipe.delete(shard)

        # Delete all elements.
        pipe.execute()

def _normalize(shard, detailed=False):
    # DRY violation.
    ret = {
        'name': shard['n'],
        'uri': shard['u'],
        'weight': int(shard['w']),
    }
    if detailed:
        ret['options'] = shard['o']

    return ret



