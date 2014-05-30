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

from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi.queues.storage.redis import utils
from marconi.queues.storage import errors
from marconi.openstack.common import timeutils

LOG = logging.getLogger(__name__)

QUEUES_SET_STORE_NAME = 'queues_set'

class QueueController(storage.Queue):
    """Implements queue resource operations using Riak.

    Queues are scoped by project, which is prefixed to the
    queue name.

    Queues ( Redis Sorted Set ):
        Id: queues_set

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
     def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self._driver = self.driver.connection
     
    def list(self, project=None, marker=None,
             limit=storage.DEFAULT_QUEUES_PER_PAGE, detailed=False):

        raise NotImplementedError

    
    def get_metadata(self, name, project=None):

        raise NotImplementedError

    
    def create(self, name, project=None):

        raise NotImplementedError

    
    def exists(self, name, project=None):

        raise NotImplementedError

    
    def set_metadata(self, name, metadata, project=None):

        raise NotImplementedError

    
    def delete(self, name, project=None):

        raise NotImplementedError

    
    def stats(self, name, project=None):

        raise NotImplementedError
