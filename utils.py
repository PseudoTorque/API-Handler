"""
Utility functions and classes for the project.
"""

from multiprocessing import Process
from time import sleep
from socket import error, socket, AF_INET, SOCK_STREAM
import errno

import dill

class Job:

    """
    A class to represent the job object.
    """

    def __init__(self, job: any, process_id: int) -> None:

        self.job = job

    def __lt__(self, other: "Job") -> bool:

        """
        Check if the job is less than another job based on the priority.
        """

        return self.job.priority < other.job.priority
    
class DillProcess(Process):

    """
    A process that uses dill to serialize and deserialize any depth objects.
    """

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._target = dill.dumps(self._target)  # Save the target function as bytes, using dill

    def run(self):

        if self._target:
            
            self._target = dill.loads(self._target)    # Unpickle the target function before executing

            self._target(*self._args, **self._kwargs)

def cycle(frequency: float) -> bool:

    """
    A function to cycle the program at a given frequency.
    """

    sleep(1/frequency)

    return True

def listener(frequency: float, host: str, port: int, connection_pool, process_pool) -> None:

        """
        A function to listen for connections and add them to the connection pool.

        Args:
            frequency (float): The frequency at which the program should cycle.
            connection_pool (dict): The dictionary to store the connections.
            process_pool (dict): The dictionary to store the processes that are assigned to the connections.
        """
        Socket = socket(AF_INET, SOCK_STREAM)

        Socket.bind((host, port))
        
        Socket.listen()

        while cycle(frequency):
            
            #accept the connection and add it to the connection pool
            #assign a key of the lowest value to the connection
            try:
                conn, _ = Socket.accept()

            except BlockingIOError:

                continue

            i = 0

            while i in connection_pool:

                i += 1

            connection_pool[i] = conn

            #assign a process to the connection
            print(connection_pool)

def decode_object(socket: any) -> any:

    """
    A function to decode the bytes into an object.
    """

    header_size = 30

    #bytes is a string of bytes that is a pickled object, the delimiters are the string BEGIN and the string END.

    header = socket.recv(header_size)

    message_length = int(header.decode("utf-8").strip())

    return dill.loads(socket.recv(message_length))

def encode_object(data: any) -> bytes:

    """
    A function to encode the object into bytes.
    """

    header_size = 30

    object_bytes = dill.dumps(data)

    out = bytes(f"{len(object_bytes):<{header_size}}", 'utf-8') + object_bytes

    return out

def pooler(frequency: float, connection_pool, job_queue) -> None:

    """
    A function to pool APIJob objects from the connections and add them to the job queue.
    """

    while cycle(frequency):

        for i in connection_pool.keys():

            conn = connection_pool[i]

            if conn.getblocking():

                conn.setblocking(False)

            try:

                job = decode_object(conn)

                print(job)

                #job_queue.put(Job(job))

            except error as e:
                
                if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:

                    continue

#essentially, the process pool is a dictionary of {assigned_process_id (same as the key in the connection pool) : process_id}
#the dispatched will take into consideration the rate_limits, will select the high priority job and put it into the job pool
#upon encountering a "CLOSE" message the dispatcher will terminate the process, remove it from the process pool and remove the process_job_queue respective to the process
#and also remove the connection from the connection pool

def dispatcher(frequency: float, job_queue, process_job_queue, connection_pool) -> None:

    """
    A function to dispatch the jobs to the process_job_queue for the respective processes.
    """

    raise NotImplementedError
       
