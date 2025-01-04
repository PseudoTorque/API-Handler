"""
All the objects used in the project.
"""

from multiprocessing.managers import SyncManager

from utils import DillProcess, JobQueue, ConnectionPool #objects
from utils import listener, poller, dispatcher #functions (for processes)

class Manager(SyncManager):
    """
    A class to manage the shared objects.
    """

    

Manager.register("JobQueue", JobQueue)

Manager.register("ConnectionPool", ConnectionPool)

class APIHandler:
    """
    A class to handle the API calls.
    """

    def __init__(self, host: str, port: int) -> None:
        
        self.spawn_manager()

        self.connection_pool = self.manager.ConnectionPool()

        self.job_queue = self.manager.JobQueue()

        self.data = self.manager.dict({
                                        "original_frequency":{"pooler":50000.0, "dispatcher":10.0},
                                        "rate_limit":{"upstox":{"default":[10, 50, 100], "current":[0, 0, 0], "last_reset":[0, 0, 0]}}
                                    })

        self.host = host

        self.port = port

        self.listener = DillProcess(target=listener, args=(self.host, self.port, self.connection_pool))

        self.listener.start()

        self.pooler = DillProcess(target=poller, args=(self.data, self.connection_pool, self.job_queue))

        self.pooler.start()

        self.dispatcher = DillProcess(target=dispatcher, args=(self.data, self.job_queue, self.connection_pool))

        self.dispatcher.start()

        self.dispatcher.join()

        self.pooler.join()

        self.listener.join()

        self.manager.join()

    def spawn_manager(self) -> None:

        self.manager = Manager()

        self.manager.start()

    


if __name__ == "__main__":

    test = APIHandler("127.0.0.1", 8080)