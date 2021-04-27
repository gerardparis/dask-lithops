import time

from distributed.worker import Worker

from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen


def lithops_function(worker_id, scheduler_address):
    print("In lithops_function... ", worker_id, " ", scheduler_address)


    # Alternative: https://docs.dask.org/en/latest/setup/python-advanced.html
    print("Create worker")
    IOLoop.clear_instance()
    loop = IOLoop()
    loop.make_current()
    worker = Worker(scheduler_ip=scheduler_address)
    
    async def do_stop(timeout=5, executor_wait=True):
        try:
            await worker.close(
                report=False,
                nanny=False,
                executor_wait=executor_wait,
                timeout=timeout,
            )
        finally:
            loop.stop()

    async def run():
        """
        Try to start worker and inform parent of outcome.
        """
        try:
            await worker
        except Exception as e:
            print(e)
            print("Failed to start worker")
        else:
            await worker.finished()
            print("Worker closed")


    try:
        loop.run_sync(run)
    except (TimeoutError, gen.TimeoutError):
        # Loop was stopped before wait_until_closed() returned, ignore
        print ("TimeoutError!")
        pass
    except KeyboardInterrupt:
        # At this point the loop is not running thus we have to run
        # do_stop() explicitly.
        loop.run_sync(do_stop)

    print(' [*] Exiting function.')

    return

