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

"""Riak storage driver configuration options."""
from oslo.config import cfg


RIAK_OPTIONS = (
    cfg.StrOpt('uri', default="riak://127.0.0.1:8087", help='Riak Server URI.'),

    cfg.StrOpt('protocol', default="pbc", help='Protocol to connect to Riak server'),

    cfg.IntOpt('max_attempts', default=1000,
               help=('Maximum number of times to retry a failed operation. '
                     'Currently only used for retrying a message post.')),

    cfg.FloatOpt('max_retry_sleep', default=0.1,
                 help=('Maximum sleep interval between retries '
                       '(actual sleep time increases linearly '
                       'according to number of attempts performed).')),

    cfg.FloatOpt('max_retry_jitter', default=0.005,
                 help=('Maximum jitter interval, to be added to the '
                       'sleep interval, in order to decrease probability '
                       'that parallel requests will retry at the '
                       'same instant.')),

    cfg.IntOpt('max_reconnect_attempts', default=10,
               help=('Maximum number of times to retry an operation that '
                     'failed due to a primary node failover.')),

    cfg.FloatOpt('reconnect_sleep', default=0.020,
                 help=('Base sleep interval between attempts to reconnect '
                       'after a primary node failover. '
                       'The actual sleep time increases exponentially (power '
                       'of 2) each time the operation is retried.')),
)

RIAK_GROUP = 'drivers:storage:riak'

def _config_options():
    return [(RIAK_GROUP, RIAK_OPTIONS)]
