import os
import time
import boto
#from boto import connect_ec2 as connect
from boto.ec2 import connect_to_region
from fabric.api import sudo, settings, env, hide
from fabric.network import disconnect_all

from gilliam_admin.scheduler import SchedulerAPI


def connect(opts, **args):
    return connect_to_region(opts.region, **args)


def get_or_make_group(conn, name):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print "Creating security group " + name
        return conn.create_security_group(name, "Gilliam EC2 group")


def get_group(conn, name):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    assert len(group) > 0
    return group[0]


def _apt_get_install(package):
    """Issue a command for installing a package using apt-get."""
    sudo('apt-get -qq install -y --force-yes ' + package)


def _wait_for_system_and_instance_status_checks(conn, instances):
    """Wait for the given instances to pass system and instance status
    checks.
    """
    #print "Waiting for instances to pass system and instance status checks..."

    instance_ids = [i.id for i in instances]
    while True:
        statuses = conn.get_all_instance_status(instance_ids)
        pending = [status for status in statuses
                   if (status.system_status.status != 'ok'
                       or status.instance_status.status != 'ok')]
        if not pending:
            break
        time.sleep(5)


def _wait_for_instances_to_become_running(conn, instances):
    """Wait for given instances to become running."""
    while True:
        for i in instances:
            i.update()
        if len([i for i in instances if i.state == 'pending']) > 0:
            time.sleep(5)
        else:
            break

def _wait_for_instances(conn, instances):
    """Wait for instances to become fully ready."""
    _wait_for_instances_to_become_running(conn, instances)
    _wait_for_system_and_instance_status_checks(conn, instances)


def _make_host_string(nodes, username='ubuntu'):
    return ','.join(['%s@%s' % (username, n.public_dns_name)
                     for n in nodes])


def wait_for_instances(conn, instances):
    while True:
        for i in instances:
            i.update()
        if len([i for i in instances if i.state == 'pending']) > 0:
            time.sleep(5)
        else:
            break
    _wait_for_system_and_instance_status_checks(conn, instances)


def _ensure_sched_group_rules(sched_group, exec_group):
    if not sched_group.rules:
        sched_group.authorize(src_group=sched_group)
        sched_group.authorize(src_group=exec_group)
        sched_group.authorize('tcp', 22, 22, '0.0.0.0/0')
        sched_group.authorize('tcp', 8000, 8000, '0.0.0.0/0')

def _ensure_exec_group_rules(exec_group, sched_group):
    if not exec_group.rules:
        exec_group.authorize(src_group=exec_group)
        exec_group.authorize(src_group=sched_group)
        exec_group.authorize('tcp', 22, 22, '0.0.0.0/0')
        exec_group.authorize('tcp', 9000, 9000, '0.0.0.0/0')
        exec_group.authorize('tcp', 10000, 11000, '0.0.0.0/0')


# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
    return (instance.state in ['pending', 'running', 'stopping', 'stopped'])


AMI_MAPPING = {
    'eu-west-1': 'ami-57b0a223'
    }


class Options(object):
    region = 'eu-west-1'
    scale = 1
    key_pair = None
    exec_instance_type = 'm1.small'
    sched_instance_type = 'm1.small'

    def __init__(self, **options):
        self.__dict__.update(options)

    @property
    def ami(self):
        return AMI_MAPPING[self.region]


class Cluster(object):
    """Representation of a Gilliam cluster."""

    def __init__(self, name, opts, sched_nodes, exec_nodes=None):
        self.name = name
        self.opts = opts
        if exec_nodes is None:
            exec_nodes = []
        self._sched_nodes = sched_nodes
        self._exec_nodes = exec_nodes

    @classmethod
    def get(cls, conn, opts, cluster_name):
        """Get an existing cluster if available."""
        reservations = conn.get_all_instances()
        sched_nodes = []
        exec_nodes = []
        for res in reservations:
            active = [i for i in res.instances if is_active(i)]
            if len(active) > 0:
                group_names = [g.name for g in res.groups]
                if (cluster_name + '-sched') in group_names:
                    sched_nodes += res.instances
                if (cluster_name + '-exec') in group_names:
                    exec_nodes += res.instances
        if sched_nodes:
            return cls(cluster_name, opts, sched_nodes, exec_nodes)
        else:
            return None

    @classmethod
    def create(cls, conn, opts, cluster_name):
        sched_group = get_or_make_group(conn, cluster_name + '-sched')
        exec_group = get_or_make_group(conn, cluster_name + '-exec')
        _ensure_sched_group_rules(sched_group, exec_group)
        _ensure_exec_group_rules(exec_group, sched_group)

        print "creating cluster %s with 1 scheduler" % (cluster_name,)

        image = conn.get_all_images(image_ids=[opts.ami])[0]
        sched_res = image.run(key_name=opts.key_pair,
                              security_groups=[sched_group],
                              instance_type=opts.sched_instance_type,
                              min_count=1,
                              max_count=1)
        _wait_for_instances(conn, sched_res.instances)
        return cls(cluster_name, opts, sched_res.instances)

    def destroy(self, conn):
        """Destroy the cluster by terminating all instances."""
        for inst in self._exec_nodes + self._sched_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.terminate()

    def _init_connections(self):
        """Initialize and make sure that we can connect to the remote
        nodes.
        """
        if self.opts.ssh_key_file:
            env.key_filename = os.path.expanduser(self.opts.ssh_key_file)

    def scale(self, conn, count):
        """Scale the cluster up or down."""

        def _scale_down(n):
            """Scale the cluster down with C{n} nodes."""
            for _ in range(n):
                inst = self._exec_nodes.pop()
                self._deregister_executor(inst.public_dns_name)
                inst.terminate()

        def _scale_up(n):
            """Scale the cluster up with C{n} nodes."""
            exec_grp = get_group(conn, self.name + '-exec')
            image = conn.get_all_images(image_ids=[self.opts.ami])[0]
            exec_res = image.run(key_name=self.opts.key_pair,
                                 security_groups=[exec_grp],
                                 instance_type=self.opts.exec_instance_type,
                                 min_count=n, max_count=n)
            nodes = exec_res.instances
            _wait_for_instances(conn, nodes)
            self._setup_executors(conn, nodes)
            self._exec_nodes.extend(nodes)

        v = count - len(self._exec_nodes)
        print "scaling cluster to %d executors (change %d)" % (count, v)
        _scale_down(-v) if v < 0 else _scale_up(v)

    def _register_executor(self, inst):
        """Register a instance."""
        api = SchedulerAPI(self._sched_nodes[0].public_dns_name)
        return api.executor_add(inst.public_dns_name, 9000, {})

    def _setup_executors(self, conn, nodes):
        """Setup stuff."""
        self._init_connections()
        try:
            with hide('commands'):
                with settings(host_string=_make_host_string(nodes),
                              command_timeout=120):
                    self._prepare_apt_repo()
                    _apt_get_install('gilliam-hypervisor')
            for node in nodes:
                self._register_executor(node)
        finally:
            with hide('status'):
                disconnect_all()

    def setup(self, conn):
        self._init_connections()
        self._setup_sched(conn)
        with hide('status'):
            disconnect_all()

    def _prepare_apt_repo(self):
        _apt_get_install('python-software-properties')
        sudo('echo "deb http://eu-west-1.ec2.archive.ubuntu.com/ubuntu/ precise multiverse" >> /etc/apt/sources.list')
        sudo('echo "deb http://gilliam-ubuntu-apt.s3.amazonaws.com precise main" >> /etc/apt/sources.list')
        sudo('apt-get -qq update ')

    def _setup_sched(self, conn):
        with hide('commands'):
            with settings(host_string=_make_host_string(self._sched_nodes),
                          command_timeout=120):
                self._prepare_apt_repo()
                sudo('apt-get -qq install -y --force-yes gilliam-scheduler')
                sudo('start gilliam-scheduler')
