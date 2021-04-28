import time

from dask_lithops import LithopsCluster
from dask.distributed import Client
from dask import delayed


def slowadd(x, y, delay=0.1):
    time.sleep(delay)
    return x + y


if __name__ == "__main__":
    config = {'lithops': {'mode': 'localhost', 'storage': 'localhost'}}

    cluster = LithopsCluster(lithops_config=config)
    cluster.scale(2)

    # Connect Dask to the cluster
    client = Client(cluster)

    # wait for workers to start running
    time.sleep(5)

    L = range(512)
    while len(L) > 1:
        L = [delayed(slowadd)(a, b) for a, b in zip(L[::2], L[1::2])]

    print(L[0].compute())
