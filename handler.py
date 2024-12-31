"""
All the objects used in the project.
"""

from queue import PriorityQueue
from multiprocessing.managers import SyncManager

from utils import DillProcess, listener, pooler, dispatcher, broadcaster

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

        self.broadcast_pool = self.manager.Queue()

        self.statistics = self.manager.dict({"buffer_length":{"pooler":10, "dispatcher":10}, 
                                             "min_frequency":{"pooler":0.1, "dispatcher":0.1}, 
                                             "original_frequency":{"pooler":500.0, "dispatcher":10.0},
                                             "rate_limit":{"upstox":{"default":[10, 50, 100], "current":[0, 0, 0], "last_reset":[0, 0, 0]}}})

        self.host = host

        self.port = port

        self.listener = DillProcess(target=listener, args=(self.host, self.port, self.connection_pool))

        self.listener.start()

        self.pooler = DillProcess(target=pooler, args=(self.statistics, self.connection_pool, self.job_queue))

        self.pooler.start()

        self.dispatcher = DillProcess(target=dispatcher, args=(self.statistics, self.job_queue, self.broadcast_pool))

        self.dispatcher.start()

        self.broadcaster = DillProcess(target=broadcaster, args=(self.broadcast_pool, self.connection_pool))

        self.broadcaster.start()

        self.broadcaster.join()

        self.dispatcher.join()

        self.pooler.join()

        self.listener.join()

        self.manager.join()

    def spawn_manager(self) -> None:

        self.manager = Manager()

        self.manager.register("PriorityQueue", PriorityQueue)

        self.manager.start()

    


if __name__ == "__main__":

    test = APIHandler("127.0.0.1", 8080)