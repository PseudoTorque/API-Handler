"""
All the objects used in the project.
"""
from socket import socket, AF_INET, SOCK_STREAM
from queue import PriorityQueue
from multiprocessing.managers import SyncManager

from requests import request, Response

from constants import Priority
from utils import DillProcess, listener, pooler


class APIJob:
    """
    A class to represent the API call (the job object). Processes will send objects of this class to be processed by the APIHandler.
    """

    def __init__(self) -> None:

        """
        Initialize the API job.
        """

        self.method = None

        self.url = None

        self.timeout: tuple[int] = None

        self.headers = {}

        self.payload = {}

        self.priority = Priority()

    def call(self) -> Response:

        """
        Make the API call through the requests library and return the response.
        """

        return request(method=self.method, url=self.url, timeout=self.timeout, headers=self.headers, data=self.payload)

class Manager(SyncManager):
    """
    A class to manage the shared objects.
    """
    pass

class APIHandler:
    """
    A class to handle the API calls.
    """

    def __init__(self, host: str, port: int) -> None:
        
        self.spawn_manager()

        self.connection_pool = self.manager.dict()

        self.job_queue = self.manager.PriorityQueue()

        self.process_pool = self.manager.dict()

        self.process_job_queue = self.manager.dict()

        self.broadcast_pool = self.manager.list()

        self.statistics = self.manager.dict()

        self.host = host

        self.port = port

        self.listener = DillProcess(target=listener, args=(1, self.host, self.port, self.connection_pool, self.process_pool))

        self.listener.start()

        self.pooler = DillProcess(target=pooler, args=(1, self.connection_pool, self.job_queue))

        self.pooler.start()

        self.pooler.join()

        self.listener.join()

        self.manager.join()

    def spawn_manager(self) -> None:

        self.manager = Manager()

        self.manager.register("PriorityQueue", PriorityQueue)

        self.manager.start()

    


if __name__ == "__main__":

    test = APIHandler("127.0.0.1", 8080)