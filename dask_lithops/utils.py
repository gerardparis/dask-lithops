"""Utility functions."""
import os
import random
import shutil
import socket
import subprocess
import string
import time

from dask.distributed import Client


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


def format_labels(labels):
    """ Convert a dictionary of labels into a comma separated string """
    if labels:
        return ",".join(["{}={}".format(k, v) for k, v in labels.items()])
    else:
        return ""


def _random_free_port(low, high, retries=20):
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while retries:
        guess = random.randint(low, high)
        try:
            conn.bind(("", guess))
            conn.close()
            return guess
        except OSError:
            retries -= 1
    raise ConnectionError("Not able to find a free port.")


async def is_comm_open(ip, port, retries=30):
    while retries > 0:
        try:
            async with Client(f"tcp://{ip}:{port}", asynchronous=True, timeout=0.5):
                return True
        except Exception:
            time.sleep(0.5)
            retries -= 1
    return False


def check_dependency(dependency):
    if shutil.which(dependency) is None:
        raise RuntimeError(
            f"Missing dependency {dependency}. "
            f"Please install {dependency} following the instructions for your OS. "
        )
