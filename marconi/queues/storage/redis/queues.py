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

from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi.queues.storage.redis import utils

LOG = logging.getLogger(__name__)

QUEUES_SET_STORE_NAME = 'queues_set'
QUEUES_LIST_STORE_NAME = 'queues_list'

class QueueController(storage.Queue):
    """Implements queue resource operations using Redis.

    Queues are scoped by project, which is prefixed to the
    queue name.

    Duplicating data because of this:

    http://tinyurl.com/multidataredis

    Queues ( Redis Set & List ):
        Id: queues

        Id                   Value
        ---------------------------------
        name      ->   <project-id_q-name>

    The set helps faster existence checks, while the list helps
    paginated retrieval of queues.

    Queue Information (Redis Hash):

        Id: <project-id_q-name>
        Id                     Value
        --------------------------------
        c               ->     count
        m               ->     metadata

    """

    def _init_pipeline(self):
        self._pipeline = self._client.pipeline()

    def init_connection(self):
        """
            Will be used during reconnection attempts.
        """
        self._client = self.driver.connection

    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self.init_connection()
        self._init_pipeline()

    def _get_queue_info(self, q_id, field):
        """
            Method to retrieve a particular field from
            the queue information.
        """
        return self._client.hmget(q_id, [field])

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def list(self, project=None, marker=None,
             limit=storage.DEFAULT_QUEUES_PER_PAGE, detailed=False):
        # Using the Redis list retrieve a range of queues.
        # Complexity O(S+N) - since redis lists are implemented
        # as linked lists.
        client = self._client
        qlist_id = utils.scope_queue_name(QUEUES_LIST_STORE_NAME, project)
        
        marker = 0 if not marker else int(marker)

        marker_next = marker
        queues = client.lrange(qlist_id, marker, marker+limit)

        # Note(prashanthr_): Might have to use caching for the
        # list of Q names.
        if len(queues) < limit:
            marker_next += len(queues)
        else:
            marker_next += limit

        def denormalizer(q_info, q_name):
            queue = {'name': utils.descope_queue_name(q_name)}
            queue['metadata'] = q_info[1]
            if detailed:
                queue['metadata'] = q_info[1]
            return queue

        yield utils.QueueListCursor(self._client, queues, denormalizer)
        yield marker_next

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get_metadata(self, name, project=None):
        q_id = utils.scope_queue_name(name, project)
        metadata = self._get_queue_info(q_id , 'm')
        return {} if utils.is_metadata_empty(metadata) else metadata

    @utils.raises_conn_error
    @utils.reset_pipeline
    def create(self, name, project=None):
        #Note (prashanthr_): Implement as a lua script.
        q_id = utils.scope_queue_name(name, project)
        qlist_id = utils.scope_queue_name(QUEUES_LIST_STORE_NAME, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)

        pipe = self._pipeline
        # Insert the queue into a the set of all queues.
        if not self._client.sadd(qset_id, q_id):
            return False

        # Create the list of queues to retrieve data by range.
        # Create a pipeline to ensure atomic inserts.
        # Create the corresponding queue information.
        q_info = {
            'c': 1,
            'm': {}
        }

        pipe.lpush(qlist_id, q_id)\
            .hmset(q_id, q_info)

        return all(map(bool, pipe.execute()))

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def exists(self, name, project=None):
        q_id = utils.scope_queue_name(name, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)
        # Note (prashanthr_): smembers is O(n)
        # try to fit in caching if possible.

        return q_id in self._client.smembers(qset_id)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def set_metadata(self, name, metadata, project=None):
        # Note(prashanthr_): Understand what is Message counter.
        q_id = utils.scope_queue_name(name, project)
        q_info = {
            'c': 1,
            'm': metadata
        }

        if self.exists(name, project):
            q_info['c'] = self._get_queue_info(q_id, 'c')

        self._client.hmset(q_id, q_info)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def delete(self, name, project=None):
        # Pipelining is used to ensure no race conditions
        # occur.

        q_id = utils.scope_queue_name(name, project)
        qlist_id = utils.scope_queue_name(QUEUES_LIST_STORE_NAME, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)

        pipe = self._pipeline

        #Note: Some problem with lrem.
        pipe.lrem(qlist_id, 1, q_id)
        pipe.srem(qset_id, q_id)
        pipe.hdel(q_id, 'c', 'm')

        print pipe.execute()


    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def stats(self, name, project=None):
        # TODO: Depends on claims controller.
        raise NotImplementedError
