# Copyright 2013 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""\
Administration and operator tool for Gilliam.

Usage: gilliam-admin [options] <command> [<args>...]

Options:
    -h, --help        Display this help text and exit
    --version         Show version and quit
    --cluster NAME    Specify cluster name

Commands:
    create            Create a new cluster
    destroy           Destroy a existing cluster
    status            Show status information about cluster
    scale             Scale cluster up or down
    
See `gilliam-admin help <command>` for more information on a specific
command.
"""

import os
import sys

from docopt import docopt

from gilliam_admin.cli import expose, main as _cli_main
from gilliam_admin import ec2


@expose("create")
def create(app_options, argv):
    """\
    Create a new cluster.

    Usage: gilliam [options] create

    Options:
      --region REGION       What EC2 region to use [default: eu-west-1]
      -k KEY, --key-pair KEY    What key-pair to use
      -i PATH, --identity PATH   SSH key identity

    Environment variables:
      AWS_EC2_SSH_KEY_FILE  Same as --identity PATH.
    """
    cluster_name = app_options.get('--cluster') or 'gilliam'
    options = docopt(create.__doc__, argv=argv)
    opts = ec2.Options(
        region=options['--region'],
        key_pair=options['--key-pair'],
        ssh_key_file=options.get('--identity', os.getenv(
                'AWS_EC2_SSH_KEY_FILE'))
     )
    conn = ec2.connect(opts)
    cluster = ec2.Cluster.create(conn, opts, cluster_name)
    cluster.setup(conn)
    cluster.scale(conn, 1)


@expose("destroy")
def destroy(app_options, argv):
    """\
    Create a new cluster.

    Usage: gilliam [options] destroy

    Options:
      --region REGION       What EC2 region to use [default: eu-west-1]

    Environment variables:
      AWS_EC2_SSH_KEY_FILE  Same as --identity PATH.
    """
    cluster_name = app_options.get('--cluster') or 'gilliam'
    options = docopt(destroy.__doc__, argv=argv)
    opts = ec2.Options(
        region=options['--region'],
     )
    conn = ec2.connect(opts)
    cluster = ec2.Cluster.get(conn, opts, cluster_name)
    if cluster is None:
        sys.exit("Cannot find a Gilliam cluster")
    cluster.destroy(conn)


@expose("scale")
def scale(app_options, argv):
    """\
    Scale the number of executors for a cluster.

    Usage: gilliam [options] scale <COUNT>

    Options:
      --region REGION       What EC2 region to use [default: eu-west-1]
    """
    cluster_name = app_options.get('--cluster') or 'gilliam'
    options = docopt(scale.__doc__, argv=argv)
    opts = ec2.Options(
        region=options['--region'],
     )
    conn = ec2.connect(opts)
    cluster = ec2.Cluster.get(conn, opts, cluster_name)
    if cluster is None:
        sys.exit("cannot find a %s cluster" % (cluster_name,))
    cluster.scale(conn, int(options['<COUNT>']))


def main():
    _cli_main(__doc__, 'gilliam-admin 0.0')
