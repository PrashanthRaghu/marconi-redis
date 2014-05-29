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

import socket
from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi.queues.storage.riak import options
from marconi.common import decorators
from marconi.queues.storage.riak import controllers
from marconi.queues.storage.riak import utils
from riak import RiakClient

LOG = logging.getLogger(__name__)

def _getRiakClient(conf):
    host, port = utils.get_hostport(conf['uri'])
    return RiakClient(protocol=conf['protocol'], host=host, http_port=port)

class DataDriver(storage.DataDriverBase):

    def __init__(self, conf, cache):
        super(DataDriver, self).__init__(conf, cache)

        opts = options.RIAK_OPTIONS

        if 'dynamic' in conf:
            names = conf[options.RIAK_GROUP].keys()
            opts = filter(lambda x: x.name not in names, opts)

        self.conf.register_opts(opts,
                                group=options.RIAK_GROUP)
        self.riak_conf = self.conf[options.RIAK_GROUP]

    def is_alive(self):
        try:
            return self.connection.ping()
        except socket.error:
            return False

    @decorators.lazy_property(write=False)
    def connection(self):
        """Redis client connection instance."""
        return _getRiakClient(self.riak_conf)

    @decorators.lazy_property(write=False)
    def queue_controller(self):
        return controllers.QueueController(self)

    @decorators.lazy_property(write=False)
    def message_controller(self):
        return controllers.MessageController(self)

    @decorators.lazy_property(write=False)
    def claim_controller(self):
        return controllers.ClaimController(self)


class ControlDriver(storage.ControlDriverBase):

    def __init__(self, conf, cache):
        super(ControlDriver, self).__init__(conf, cache)

        self.conf.register_opts(options.RIAK_OPTIONS,
                                group=options.RIAK_GROUP)

        self.riak_conf = self.conf[options.RIAK_GROUP]

    @decorators.lazy_property(write=False)
    def connection(self):
        """Redis client connection instance."""
        return _getRiakClient(self.riak_conf)

    @property
    def shards_controller(self):
        return controllers.ShardsController(self)

    @property
    def catalogue_controller(self):
        return controllers.CatalogueController(self)