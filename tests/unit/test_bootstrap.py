# Copyright (c) 2013 Rackspace, Inc.
#
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

from marconi.common import errors
from marconi.queues import bootstrap
from marconi.queues.storage import pipeline
from marconi.queues.storage import pooling
from marconi.queues.storage import sqlalchemy
from marconi.queues.transport import wsgi
from marconi.tests import base


class TestBootstrap(base.TestBase):

    def _bootstrap(self, conf_file):
        self.conf = self.load_conf(conf_file)
        return bootstrap.Bootstrap(self.conf)

    def test_storage_invalid(self):
        bootstrap = self._bootstrap('drivers_storage_invalid.conf')
        self.assertRaises(errors.InvalidDriver,
                          lambda: bootstrap.storage)

    def test_storage_sqlalchemy(self):
        bootstrap = self._bootstrap('wsgi_sqlalchemy.conf')
        self.assertIsInstance(bootstrap.storage, pipeline.DataDriver)
        self.assertIsInstance(bootstrap.storage._storage,
                              sqlalchemy.DataDriver)

    def test_storage_sqlalchemy_pooled(self):
        """Makes sure we can load the pool driver."""
        bootstrap = self._bootstrap('wsgi_sqlalchemy_pooled.conf')
        self.assertIsInstance(bootstrap.storage._storage, pooling.DataDriver)

    def test_transport_invalid(self):
        bootstrap = self._bootstrap('drivers_transport_invalid.conf')
        self.assertRaises(errors.InvalidDriver,
                          lambda: bootstrap.transport)

    def test_transport_wsgi(self):
        bootstrap = self._bootstrap('wsgi_sqlalchemy.conf')
        self.assertIsInstance(bootstrap.transport, wsgi.Driver)
