from dask_lithops import LithopsCluster
from dask.distributed import Client
import dask.array as da


if __name__ == "__main__":
    config = {'lithops': {'mode': 'localhost', 'storage': 'localhost'}}

    cluster = LithopsCluster(lithops_config=config)
    cluster.scale(4)

    # Connect Dask to the cluster
    client = Client(cluster)

    # Create a large array and calculate the mean
    array = da.ones((1000, 1000, 1000))
    print(array.mean().compute())  # Should print 1.0
