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

import redis
from marconi.queues.storage import errors
from marconi.openstack.common import log as logging
import functools


LOG = logging.getLogger(__name__)

def is_metadata_empty(metadata):
    """
        Check if the metadata for a queue is empty.
        Empty metadata is represented as '{}'
    """
    return metadata is '{}'

def descope_queue_name(scoped_name):
    """
        Returns the queue name from the scoped name
        which is of the form project-id_queue-name
    """
    return scoped_name.split('_')[1]

def normalize_none_str(string_or_none):
    """Returns '' IFF given value is None, passthrough otherwise.

    This function normalizes None to the empty string to facilitate
    string concatenation when a variable could be None.
    """
    #Note(prashanthr_) : Try to reuse this utility. Violates DRY
    return '' if string_or_none is None else string_or_none


def scope_queue_name(queue=None, project=None):
    """Returns a scoped name for a queue based on project and queue.

    If only the project name is specified, a scope signifying "all queues"
    for that project is returned. If neither queue nor project are
    specified, a scope for "all global queues" is returned, which
    is to be interpreted as excluding queues scoped by project.

    :returns: '{project}_{queue}' if project and queue are given,
        '{project}/' if ONLY project is given, '_{queue}' if ONLY
        queue is given, and '_' if neither are given.
    """
     #Note(prashanthr_) : Try to reuse this utility. Violates DRY

    # NOTE(kgriffs): Concatenation is faster than format, and
    # put project first since it is guaranteed to be unique.
    return normalize_none_str(project) + '_' + normalize_none_str(queue)

def raises_conn_error(func):
    """Handles the Redis ConnectionFailure error.

    This decorator catches Redis's ConnectionError
    and raises Marconi's ConnectionError instead.
    """
    # Note(prashanthr_) : Try to reuse this utility. Violates DRY
    # Can pass exception type into the decorator and create a
    # storage level utility.

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError as ex:
            LOG.exception(ex)
            raise errors.ConnectionError()

    return wrapper


def retries_on_autoreconnect(func):
    """Causes the wrapped function to be re-called on AutoReconnect.

    This decorator catches MongoDB's AutoReconnect error and retries
    the function call.

    .. Note::
       Assumes that the decorated function has defined self.driver.mongodb_conf
       so that `max_reconnect_attempts` and `reconnect_sleep` can be taken
       into account.

    .. Warning:: The decorated function must be idempotent.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # TODO(kgriffs): Figure out a way to not have to rely on the
        # Note(prashanthr_) : Try to reuse this utility. Violates DRY
        # Can pass config parameters into the decorator and create a
        # storage level utility.

        max_attemps = self.driver.redis_conf.max_reconnect_attempts
        sleep_sec = self.driver.redis_conf.reconnect_sleep

        for attempt in range(max_attemps):
            try:
                self.init_connection()
                return func(self, *args, **kwargs)
                break

            except redis.exceptions.ConnectionError as ex:
                LOG.warn(_(u'Caught AutoReconnect, retrying the '
                           'call to {0}').format(func))

                time.sleep(sleep_sec * (2 ** attempt))
        else:
            LOG.error(_(u'Caught AutoReconnect, maximum attempts '
                        'to {0} exceeded.').format(func))

            raise ex

    return wrapper

def reset_pipeline(func):
    """
        Methods using pipeline need to reset the pipeline
        after usage.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        ret_val = func(self, *args, **kwargs)
        self._pipeline.reset()
        return ret_val

    return wrapper

class QueueListCursor(object):

    def __init__(self, client, queues, denormalizer):
        self.queue_iter = iter(queues)
        self.denormalizer = denormalizer
        self.client = client

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.queues)

    @raises_conn_error
    def next(self):
        curr = next(self.queue_iter)
        queue = self.client.hmget(curr,
                                  ['c', 'm'])
        return self.denormalizer(queue, curr)