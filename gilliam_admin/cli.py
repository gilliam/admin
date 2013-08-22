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
    --cluster <NAME>  Specify cluster name

Commands:
    create            Create a new cluster
    destroy           Destroy a existing cluster
    status            Show status information about cluster
    scale             Scale cluster up or down
    
See `gilliam-admin help <command>` for more information on a specific
command.
"""

import sys

from docopt import docopt
from textwrap import dedent


class CommandRegistry(dict):

    def expose(self, name):
        """Expose a function as a command."""
        def decl(f):
            self[name] = f
            return f
        return decl

    def lookup(self, name):
        """Lookup a command from the registry."""
        return self.get(name)


COMMAND_REGISTRY = CommandRegistry()
expose = COMMAND_REGISTRY.expose


@expose("help")
def help(app_options, argv):
    """\
    Display help for a command.

    Usage: gilliam-admin help [COMMAND]
    """
    options = docopt(help.__doc__, argv=argv)
    if not options['COMMAND']:
        stdout.write(dedent(__doc__))

    command = COMMAND_REGISTRY.lookup(options['COMMAND'])
    if command is None:
        sys.exit("gilliam-admin: help: %s: no such command" % (
                options['COMMAND'],))
    else:
        stdout.write(dedent(command.__doc__))


def main(doc, version):
    options = docopt(__doc__, version=version, options_first=True)
    cmdname = options.get('<command>', 'help')
    command = COMMAND_REGISTRY.lookup(cmdname)
    if command is None:
        sys.exit("Unknown command")
    try:
        command(options, [cmdname] + options['<args>'])
    except RuntimeError, re:
        sys.exit(str(re))
