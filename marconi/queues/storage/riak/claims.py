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

import marconi.openstack.common.log as logging
from marconi.queues.storage.redis import utils
from marconi.queues import storage
from marconi.openstack.common import timeutils
from marconi.queues.storage import errors

LOG = logging.getLogger(__name__)

QUEUE_CLAIMS_SUFFIX = 'claims'
CLAIM_MESSAGES_SUFFIX = 'messages'

class ClaimController(storage.Claim):
    """Implements claim resource operations using Riak.

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

    def _exists(self, queue, claim_id, project, delete=True):
        # Note : Handle expired claims here.
        client = self._client
        qclaims_set_id = utils.scope_claims_set(queue, project
                                                  , QUEUE_CLAIMS_SUFFIX)
        # Return False if no such claim exists
        if not client.sismember(qclaims_set_id, claim_id):
            return False

        # If the claim exists delete if it is expired
        # and return False
        claim = client.hgetall(claim_id)
        c_expires = int(claim['e.g'])
        now = timeutils.utcnow_ts()

        if now > c_expires and delete:
            self.delete(queue, claim_id, project)
            return False

        return True


    def _get_claimed_messages(self, claim_id):
        return self._client.smembers(claim_id)

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

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get(self, queue, claim_id, project=None):

        client = self._client
        if not self._exists(queue, claim_id, project):
            raise errors.ClaimDoesNotExist(claim_id, queue, project)

        claim_messages = utils.scope_claim_messages(claim_id,
                                                    CLAIM_MESSAGES_SUFFIX)

        ids = self._get_claimed_messages(claim_messages)

        msgs = [client.hgetall(id) for id in ids]

        claim_meta = client.hgetall(claim_id)

        now = timeutils.utcnow_ts()
        update_time = int(claim_meta['e']) - int(claim_meta['t'])
        age = now - update_time

        claim_meta = {
            'age': int(age),
            'ttl': claim_meta['t'],
            'id': str(claim_meta['id']),
        }

        return (claim_meta, msgs)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def create(self, queue, metadata, project=None,
               limit=storage.DEFAULT_MESSAGES_PER_CLAIM):
        qclaims_set_id = utils.scope_claims_set(queue, project
                                                  , QUEUE_CLAIMS_SUFFIX)

        claim_id = utils.generate_uuid()
        now = timeutils.utcnow_ts()
        ttl = metadata['ttl']
        grace = metadata['grace']
        claim_messages = utils.scope_claim_messages(claim_id,
                                                    CLAIM_MESSAGES_SUFFIX)
        claim_info = {
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
        messages_list = list(next(messages))
        ids = [message['id'] for message in messages_list]

        if len(ids) == 0:
            return (None,iter([]))

        message_info={
            'c': claim_id,
            'c.e': now + ttl
        }

        # Update the claim id and claim expiration info
        # for all the messages.
        for message_id in ids:
            pipe.sadd(claim_messages, message_id)
            pipe.hmset(message_id, message_info)

        pipe.execute()
        return claim_id, messages_list

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def update(self, queue, claim_id, metadata, project=None):

        client = self._client
        pipe = self._pipeline

        if not self._exists(queue, claim_id, project):
            raise errors.ClaimDoesNotExist(claim_id, queue, project)

        now = timeutils.utcnow_ts()
        ttl = int(metadata.get('ttl', 60))
        grace = int(metadata.get('grace', 60))
        expires = now + ttl
        claim_messages = utils.scope_claim_messages(claim_id,
                                                    CLAIM_MESSAGES_SUFFIX)

        ids = self._get_claimed_messages(claim_messages)

        claim_info = {
            't': ttl,
            'g': grace,
            'e': expires,
            'e.g': expires + grace
        }

        message_info = {
            'c.e': expires
        }

        # Update the claim id and claim expiration info
        # for all the messages.
        pipe.hmset(claim_id, claim_info)
        for message_id in ids:
            pipe.hmset(message_id, message_info)

        pipe.execute()

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def delete(self, queue, claim_id, project=None):
        qclaims_set_id = utils.scope_claims_set(queue, project
                                                  , QUEUE_CLAIMS_SUFFIX)
        pipe = self._pipeline

        # delete:False avoids an infinite recursive case.
        if not self._exists(queue, claim_id, project, delete=False):
            raise errors.ClaimDoesNotExist(claim_id, queue, project)

        now = timeutils.utcnow_ts()
        claim_messages = utils.scope_claim_messages(claim_id,
                                                    CLAIM_MESSAGES_SUFFIX)

        ids = self._get_claimed_messages(claim_messages)

        # Reset values of the claimed messages.
        message_info = {
            'c': None,
            'c.e': now
        }

        # Update the claim id and claim expiration info
        # for all the messages.
        pipe.srem(qclaims_set_id, claim_id)
        pipe.delete(claim_id)
        pipe.delete(claim_messages)

        for message_id in ids:
            pipe.hmset(message_id, message_info)

        pipe.execute()
