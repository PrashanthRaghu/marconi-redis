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

from oslo.config import cfg
from stevedore import driver

from marconi.common import decorators
from marconi.common import errors
from marconi.openstack.common.cache import cache as oslo_cache
from marconi.openstack.common import log
from marconi.queues.storage import pipeline
from marconi.queues.storage import pooling
from marconi.queues.storage import utils as storage_utils

LOG = log.getLogger(__name__)

_CLI_OPTIONS = (
    cfg.BoolOpt('daemon', default=False,
                help='Run Marconi server in background.'),
)

# NOTE (Obulpathi): Register daemon command line option for
# marconi-server
CONF = cfg.CONF
CONF.register_cli_opts(_CLI_OPTIONS)

_GENERAL_OPTIONS = (
    cfg.BoolOpt('pooling', default=False,
                help=('Enable pooling across multiple storage backends. ',
                      'If pooling is enabled, the storage driver ',
                      'configuration is used to determine where the ',
                      'catalogue/control plane data is kept.'),
                deprecated_opts=[cfg.DeprecatedOpt('pooling')]),
    cfg.BoolOpt('admin_mode', default=False,
                help='Activate endpoints to manage pool registry.'),
)

_DRIVER_OPTIONS = (
    cfg.StrOpt('transport', default='wsgi',
               help='Transport driver to use.'),
    cfg.StrOpt('storage', default='sqlite',
               help='Storage driver to use.'),
)

_DRIVER_GROUP = 'drivers'


def _config_options():
    return [(None, _GENERAL_OPTIONS),
            (_DRIVER_GROUP, _DRIVER_OPTIONS)]


class Bootstrap(object):
    """Defines the Marconi bootstrapper.

    The bootstrap loads up drivers per a given configuration, and
    manages their lifetimes.
    """

    def __init__(self, conf):
        self.conf = conf
        self.conf.register_opts(_GENERAL_OPTIONS)
        self.conf.register_opts(_DRIVER_OPTIONS, group=_DRIVER_GROUP)
        self.driver_conf = self.conf[_DRIVER_GROUP]

        log.setup('marconi')

    @decorators.lazy_property(write=False)
    def storage(self):
        LOG.debug(u'Loading storage driver')

        if self.conf.pooling:
            LOG.debug(u'Storage pooling enabled')
            storage_driver = pooling.DataDriver(self.conf, self.cache,
                                                self.control)
        else:
            storage_driver = storage_utils.load_storage_driver(
                self.conf, self.cache)

        LOG.debug(u'Loading storage pipeline')
        return pipeline.DataDriver(self.conf, storage_driver)

    @decorators.lazy_property(write=False)
    def control(self):
        LOG.debug(u'Loading storage control driver')
        return storage_utils.load_storage_driver(self.conf, self.cache,
                                                 control_mode=True)

    @decorators.lazy_property(write=False)
    def cache(self):
        LOG.debug(u'Loading proxy cache driver')
        try:
            oslo_cache.register_oslo_configs(self.conf)
            mgr = oslo_cache.get_cache(self.conf.cache_url)
            return mgr
        except RuntimeError as exc:
            LOG.exception(exc)
            raise errors.InvalidDriver(exc)

    @decorators.lazy_property(write=False)
    def transport(self):
        transport_name = self.driver_conf.transport
        LOG.debug(u'Loading transport driver: %s', transport_name)

        args = [
            self.conf,
            self.storage,
            self.cache,
            self.control,
        ]

        try:
            mgr = driver.DriverManager('marconi.queues.transport',
                                       transport_name,
                                       invoke_on_load=True,
                                       invoke_args=args)
            return mgr.driver
        except RuntimeError as exc:
            LOG.exception(exc)
            raise errors.InvalidDriver(exc)

    def run(self):
        self.transport.listen()
