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

    def list(self, marker=None, limit=storage.DEFAULT_SHARDS_PER_PAGE,
             detailed=False):
        """Lists all registered shards.

        :param marker: used to determine which shard to start with
        :type marker: six.text_type
        :param limit: (Default 10) Max number of results to return
        :type limit: int
        :param detailed: whether to include options
        :type detailed: bool
        :returns: A list of shards - name, weight, uri
        :rtype: [{}]
        """
        raise NotImplementedError

   
    def create(self, name, weight, uri, options=None):
        """Registers a shard entry.

        :param name: The name of this shard
        :type name: six.text_type
        :param weight: the likelihood that this shard will be used
        :type weight: int
        :param uri: A URI that can be used by a storage client
        (e.g., pymongo) to access this shard.
        :type uri: six.text_type
        :param options: Options used to configure this shard
        :type options: dict
        """
        raise NotImplementedError

   
    def get(self, name, detailed=False):
        """Returns a single shard entry.

        :param name: The name of this shard
        :type name: six.text_type
        :param detailed: Should the options data be included?
        :type detailed: bool
        :returns: weight, uri, and options for this shard
        :rtype: {}
        :raises: ShardDoesNotExist if not found
        """
        raise NotImplementedError

   
    def exists(self, name):
        """Returns a single shard entry.

        :param name: The name of this shard
        :type name: six.text_type
        :returns: True if the shard exists
        :rtype: bool
        """
        raise NotImplementedError

   
    def delete(self, name):
        """Removes a shard entry.

        :param name: The name of this shard
        :type name: six.text_type
        :rtype: None
        """
        raise NotImplementedError

   
    def update(self, name, **kwargs):
        """Updates the weight, uris, and/or options of this shard

        :param name: Name of the shard
        :type name: text
        :param kwargs: one of: `uri`, `weight`, `options`
        :type kwargs: dict
        :raises: ShardDoesNotExist
        """
        raise NotImplementedError

   
    def drop_all(self):
        """Deletes all shards from storage."""
        raise NotImplementedError

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



