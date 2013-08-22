import json
import requests

def urlchild(base_url, *args):
    return base_url + ''.join([('/%s' % arg) for arg in args])


class SchedulerAPI(object):
    """Abstraction that provides functions to talk to the orchestrator
    using its REST API.
    """

    def __init__(self, endpoint):
        if not endpoint.startswith("http://"):
            endpoint = "http://%s:8000" % (endpoint,)
        self.endpoint = endpoint

    def executor_add(self, host, port, options):
        """Add a executor to the scheduler.

        @param host: Address or IP address of the host.
        
        @param port: TCP port where the executor exposes its API.

        @param options: Options for the hypervisor host.
        """
        request = {'host': host, 'port': port, 'capacity': 1,
                   'options': options}
        response = requests.post(urlchild(self.endpoint, 'hypervisor'),
                                 data=json.dumps(request))
        response.raise_for_status()
        return response.json()

