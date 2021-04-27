import asyncio
import copy
import getpass
import logging
import os
import time
import uuid
import warnings

import yaml
import dask
import dask.distributed
import distributed.security
from distributed.deploy import SpecCluster, ProcessInterface
from distributed.utils import Log, Logs

import lithops
from .lithops_function import lithops_function

from .utils import (
    get_ip,
)

logger = logging.getLogger(__name__)

SCHEDULER_PORT = 8786


class LW(ProcessInterface):
    """A superclass for Lithops Workers
    See Also
    --------
    Worker
    """

    def __init__(
        self,
        cluster,
        loop=None,
        lithops_executor=None,
        lithops_function=None,
        **kwargs
    ):
        self.cluster = cluster
        self.name = None
        self.loop = loop

        self.lithops_executor = lithops_executor
        self.lithops_function = lithops_function
        self._future = None
        self.scheduler_address = None
        self.kwargs = kwargs
        super().__init__()

    @property
    def cluster_name(self):
        return "lithops-cluster-1"

    async def start(self, **kwargs):
        data = {"worker_id": 1, "scheduler_address": self.scheduler_address} #FIXME
        self._future = self.lithops_executor.call_async(self.lithops_function, data)

        return await super().start(**kwargs)

    async def close(self, **kwargs):
        await super().close(**kwargs)
        
        #if self._future:
        #    self.lithops_executor.get_result(self._future)

    async def logs(self):
        log = ""
        return Log(log)

    async def describe_pod(self):
        self._pod = await self.core_api.read_namespaced_pod(
            self._pod.metadata.name, self.namespace
        )
        return self._pod

    def __repr__(self):
        return "<LW %s: status=%s>" % (type(self).__name__, self.status)


class Worker(LW):
    """A Remote Dask Worker running in a Lithops worker
    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    name (optional):
        The name passed to the dask-worker CLI at creation time.
    """

    def __init__(self, scheduler: str, name=None, **kwargs):
        super().__init__(**kwargs)

        self.scheduler_address = scheduler

        # TODO name


class LithopsCluster(SpecCluster):
    """Launch a Dask cluster using Lithops

    Parameters
    ----------
    n_workers: int
        Number of workers on initial launch.
        Use ``scale`` to change this number in the future
    host: str
        Listen address for local scheduler.  Defaults to 0.0.0.0
    port: int
        Port of local scheduler
    **kwargs: dict
        Additional keyword arguments to pass to SpecCluster

    Examples
    --------
    >>> from dask_lithops import LithopsCluster
    >>> cluster = LithopsCluster(pod_spec)
    >>> cluster.scale(10)

    Rather than explicitly setting a number of workers you can also ask the
    cluster to allocate workers dynamically based on current workload

    >>> cluster.adapt()

    You can pass this cluster directly to a Dask client

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    You can verify that your local environment matches your worker environments
    by calling ``client.get_versions(check=True)``.  This will raise an
    informative error if versions do not match.

    >>> client.get_versions(check=True)

    See Also
    --------
    LithopsCluster.adapt
    """

    def __init__(
        self,
        n_workers=None,
        host=None,
        port=None,
        env=None,
        lithops_config=None,
        **kwargs
    ):

        self.lithops_config = lithops_config

        if n_workers is None:
            self._n_workers = 1
        else:
            self._n_workers = n_workers
        if host is None:
            self.host = get_ip()
        else:
            self.host = host
        if port is None:
            self.port = 8786
        else:
            self.port = port
        self._protocol = "tcp://"

        self._interface = None
        self._security = distributed.security.Security()
        self._dashboard_address = ":8787"

        self.kwargs = kwargs
        super().__init__(**self.kwargs)


    async def _start(self):
        self.lithops_executor = lithops.FunctionExecutor(config=self.lithops_config)

        common_options = {
            "cluster": self,
            "loop": self.loop,
            "lithops_executor": self.lithops_executor,
            "lithops_function": lithops_function
        }

        # local scheduler:
        self.scheduler_spec = {
            "cls": dask.distributed.Scheduler,
            "options": {
                "protocol": self._protocol,
                "interface": self._interface,
                "host": self.host,
                "port": self.port,
                "dashboard_address": self._dashboard_address,
                "security": self._security,
            },
        }

        self.new_spec = {
            "cls": Worker,
            "options": {**common_options},
        }
        self.worker_spec = {i: self.new_spec for i in range(self._n_workers)}

        self.name = "lithops-cluster-2"

        await super()._start()

    
    def scale(self, n):
        # A shim to maintain backward compatibility
        # https://github.com/dask/distributed/issues/3054
        #maximum = dask.config.get("kubernetes.count.max")
        #if maximum is not None and maximum < n:
        #    logger.info(
        #        "Tried to scale beyond maximum number of workers %d > %d", n, maximum
        #    )
        #    n = maximum
        # TODO
        return super().scale(n)

    async def _logs(self, scheduler=True, workers=True):
        """Return logs for the scheduler and workers
        Parameters
        ----------
        scheduler : boolean
            Whether or not to collect logs for the scheduler
        workers : boolean or Iterable[str], optional
            A list of worker addresses to select.
            Defaults to all workers if `True` or no workers if `False`
        Returns
        -------
        logs: Dict[str]
            A dictionary of logs, with one item for the scheduler and one for
            each worker
        """
        logs = Logs()

        if scheduler:
            logs["Scheduler"] = await self.scheduler.logs()

        if workers:
            worker_logs = await asyncio.gather(
                *[w.logs() for w in self.workers.values()]
            )
            for key, log in zip(self.workers, worker_logs):
                logs[key] = log

        return logs
