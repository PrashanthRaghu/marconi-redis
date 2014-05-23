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

import marconi.openstack.common.log as logging
from marconi.queues.storage.redis import utils
from marconi.queues import storage
from marconi.openstack.common import timeutils

LOG = logging.getLogger(__name__)

QUEUE_CLAIMS_SUFFIX = 'claims'

class ClaimController(storage.Claim):
    """Implements claim resource operations using Redis.

    Redis Data Structures:
    ---------------------
    Claims list ( Redis Set ) contains claim ids

    scope: <project-id_q-name>

        Name                Field
        -------------------------
        claim_ids               m

    Messages(Redis Hash):
        Name                Field
        -------------------------
        ttl              ->     t
        grace            ->     g
        body             ->     b
        claim            ->     c
        client uuid      ->     u
    """

    def _exists(self, queue, project):

    def _init_pipeline(self):
        self._pipeline = self._client.pipeline()

    def init_connection(self):
        """
            Will be used during reconnection attempts.
        """
        self._client = self.driver.connection

    def __init__(self, *args, **kwargs):
        super(ClaimController, self).__init__(*args, **kwargs)
        self.init_connection()
        self._init_pipeline()
        self.message_ctrl = self.driver.message_controller

    def get(self, queue, claim_id, project=None):

        raise NotImplementedError

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def create(self, queue, metadata, project=None,
               limit=storage.DEFAULT_MESSAGES_PER_CLAIM):
        qclaims_set_id = utils.scope_messages_set(queue, project, QUEUE_CLAIMS_SUFFIX)
        claim_id = utils.generate_uuid()
        now = timeutils.utcnow_ts()
        ttl =  metadata['ttl']
        grace = metadata['grace']

        claim_info ={
            'id': claim_id,
            't': ttl,
            'g': grace,
            'e': now + ttl,
            'e.g': now + ttl + grace
        }

        # Create the claim and load the metadata information.
        pipe = self._pipeline
        pipe.sadd(qclaims_set_id, claim_id)
        pipe.hmset(claim_id, claim_info)
        pipe.execute()

        # Manual resetting required here.
        pipe.reset()

        messages = self.message_ctrl._active(queue, project=project,
                                limit=limit)
        messages_list = list(messages)
        ids = [message['id'] for message in messages_list]

        if len(ids) == 0:
            return (None,iter([]))

        message_info={
            'c': claim_id,
            'c.e': now
        }

        # Update the claim id and claim expiration info
        # for all the messages.
        for message_id in ids:
            pipe.hmset(message_id, message_info)

        pipe.execute()
        return (claim_id, messages_list)

    def update(self, queue, claim_id, metadata, project=None):
        raise NotImplementedError

    def delete(self, queue, claim_id, project=None):
        raise NotImplementedError