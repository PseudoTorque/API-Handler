"""
Utility functions and classes for the project.
"""

from multiprocessing import Process, Pipe
from time import sleep, time
from socket import error, socket, AF_INET, SOCK_STREAM
import errno

import dill

from exceptions import JobQueueEmpty, BroadcastQueueEmpty, RateLimitsNotDefined
from objects import InternalInterrupt, Priority, RateLimitExceeded, Result, Status, States, Reasons, Details, APIJob, Dummy, Job, Signal, Close, SwitchToBatch, SwitchToStream, Rename

def reformat_object(obj: any) -> any:

    """
    A function to reformat the object into the appropriate object.
    """

    job_dict = obj.__dict__

    if isinstance(obj, Signal):

        if isinstance(obj, Close):

            obj = Close()

        elif isinstance(obj, SwitchToBatch):

            obj = SwitchToBatch(job_dict["num_processes"])

        elif isinstance(obj, SwitchToStream):

            obj = SwitchToStream()

        elif isinstance(obj, Rename):

            obj = Rename(job_dict["new_name"])

    else:

            obj = Dummy(**job_dict)

    obj.priority = Priority(**job_dict["priority"].__dict__)

    return obj

def decode_object(sock: socket) -> any:

    """
    A function to decode the bytes into an object.
    """

    try:

        if sock.getblocking():

            sock.setblocking(False)

        header_size = 30

        #bytes is a string of bytes that is a pickled object.

        header = sock.recv(header_size)

        header = header.decode("utf-8").strip()

        if header != "":

            message_length = int(header)

            #return the object
            obj = dill.loads(sock.recv(message_length))

            return obj
        
        else:

            return None
    
    except error as e:
                
        if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:

            return False

def encode_object(data: any) -> bytes:

    """
    A function to encode the object into bytes.
    """

    header_size = 30

    object_bytes = dill.dumps(data)

    out = bytes(f"{len(object_bytes):<{header_size}}", 'utf-8') + object_bytes

    return out

class JobQueue:

    """
    A class to represent the job queue, the job queue is a list of jobs that are sorted by priority, some functions may override this behaviour.
    """

    def __init__(self) -> None:

        self.buffer: list[Job] = []

    def get_buffer(self) -> list[Job]:

        """
        A function to get the buffer of the job queue.

        Returns:
            list[Job]: The buffer of the job queue.
        """

        return self.buffer
    
    def put(self, job: Job) -> None:

        """
        A function to put a job into the job queue based on the priority. 
        The job queue is sorted by priority, the highest priority job is at the start of the list.

        Args:
            job (Job): The job to put into the job queue.
        """

        for index, other_job in enumerate(self.buffer):

            if job > other_job:

                self.buffer.insert(index, job)

                return
            
        self.buffer.append(job)
    
    def get(self, block: bool = True) -> Job:

        """
        A function to get the job from the job queue.

        Args:
            block (bool): Whether to block the function until a job is available.

        Returns:
            Job: The job from the job queue.

        Raises:
            JobQueueEmpty: If the job queue is empty and the function is not blocking.
        """

        if block:
            
            while self.buffer == []:

                sleep(0.1)

            return self.buffer.pop(0)
        
        else:

            if self.buffer == []:

                raise JobQueueEmpty("The job queue is empty")
            
            else:

                return self.buffer.pop(0)
    
    def put_for_cycle(self, job: Job) -> None:

        """
        A function to put a job into the job queue for cycling purposes.
        """

        self.buffer.append(job)

class BroadcastQueue:

    """
    A class to represent the broadcast queue, the broadcast queue is a list of results/statuses.
    """

    def __init__(self) -> None:

        self.buffer: list[Result|Status] = []

    def get_buffer(self) -> list[Result|Status]:

        """
        A function to get the buffer of the job queue.

        Returns:
            list[Result|Status]: The buffer of the broadcast queue.
        """

        return self.buffer
    
    def put(self, result: Result|Status) -> None:

        """
        A function to put a result/status into the broadcast queue.

        Args:
            result (Result|Status): The result/status to put into the broadcast queue.
        """

        self.buffer.append(result)
    
    def get(self, block: bool = True) -> Result|Status:

        """
        A function to get the result/status from the broadcast queue.

        Args:
            block (bool): Whether to block the function until a result/status is available.

        Returns:
            Result|Status: The result/status from the broadcast queue.

        Raises:
            BroadcastQueueEmpty: If the broadcast queue is empty and the function is not blocking.
        """

        if block:
            
            while self.buffer == []:

                sleep(0.1)

            return self.buffer.pop(0)
        
        else:

            if self.buffer == []:

                raise BroadcastQueueEmpty("The broadcast queue is empty")
            
            else:

                return self.buffer.pop(0)

class ClientSocket:

    """
    A class to represent a client socket.
    """

    def __init__(self, connection: socket, client_id: str) -> None:

        """
        A function to initialize the client socket.
        """

        self.connection: socket = connection

        self.client_id: str = client_id

    def send(self, data: any) -> None:

        """
        A function to send data through the socket.
        """

        self.connection.send(encode_object(data))

    def recv(self) -> any:

        """
        A function to receive data from the client socket.
        """

        return decode_object(self.connection)

class ConnectionPool:

    """
    A class to represent the connection pool.
    """

    def __init__(self) -> None: #initialize the connection pool

        self.pool: list[ClientSocket] = [] #list of client sockets

    def get_pool(self) -> list[ClientSocket]:

        """
        A function to get the pool of client sockets.
        """

        return self.pool

    def get_lowest_client_id(self) -> str:

        """
        A function to get the lowest available (free) client id in the connection pool.
        """

        client_ids_integer = sorted([int(i.client_id.split("_")[1]) for i in self.pool if "_" in i.client_id])

        if client_ids_integer == []:

            return "client_0"
 
        return "client_" + str(client_ids_integer[-1]+1)

    def get_client_socket(self, client_id: str) -> ClientSocket:

        """
        A function to get the client socket from the connection pool.

        Args:
            client_id (str): The client id to get the client socket for.

        Returns:
            ClientSocket: The client socket for the given client id.
        """

        for i in self.pool:

            if i.client_id == client_id:

                return i
        
        return None

    def pop_client_socket(self, client_id: str) -> ClientSocket:

        """
        A function to pop the client socket from the connection pool.

        Args:
            client_id (str): The client id to pop the client socket for.

        Returns:
            ClientSocket: The client socket for the given client id.
        """

        for index, i in enumerate(self.pool):

            if i.client_id == client_id:
                
                return self.pool.pop(index)
        
        return None

    def add_client_socket(self, client_socket: socket) -> str:

        """
        A function to add a client socket to the connection pool.

        Args:
            client_socket (socket): The client socket to add to the connection pool.

        Returns:
            str: The client id allocated to the new client socket.
        """

        client_id = self.get_lowest_client_id()

        self.pool.append(ClientSocket(client_socket, client_id))

        return client_id

    def rename_client_socket(self, client_id: str, new_client_id: str) -> None:

        """
        A function to rename a client socket in the connection pool.

        Args:
            client_id (str): The client id to rename.
            new_client_id (str): The new client id to rename to.
        """

        target = self.get_client_socket(client_id)

        target.client_id = new_client_id

    def close_client_socket(self, client_id: str) -> None:

        """
        A function to close a client socket in the connection pool.

        Args:
            client_id (str): The client id to close.
        """

        self.pop_client_socket(client_id)

    def poll(self, job_queue: JobQueue) -> None:

        """
        A function to poll the client sockets for data and add them to the job queue.
        """

        to_close = []

        for client_socket in self.pool:

            data = client_socket.recv()

            if data is None:

                to_close.append(client_socket.client_id)

            else:

                if data:

                    job_queue.put(Job(reformat_object(data), client_socket.client_id))

                    #debugging
                    print("jp: " + str(data.priority.group_priority * 10 + data.priority.internal_priority) + " cid: " + str(client_socket.client_id))
                
        for client_id in to_close:

            #Clients have been closed gracefully, may send an update to the supervisor process as well
            self.close_client_socket(client_id)

class ServerSocket:

    """
    A class to represent the server socket.
    """

    def __init__(self, host: str, port: int) -> None:

        self.server_connection: socket = socket(AF_INET, SOCK_STREAM)

        self.server_connection.bind((host, port))

        self.server_connection.listen()
    
    def main(self, connection_pool: ConnectionPool) -> None:

        """
        A function to listen for connections and add them to the connection pool.
        """

        while True:

            conn, _ = self.server_connection.accept()

            _ = connection_pool.add_client_socket(conn)

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

def listener(host: str, port: int, connection_pool: ConnectionPool) -> None:

        """
        A function to listen for connections and add them to the connection pool.

        Args:
            frequency (float): The frequency at which the program should cycle.
            connection_pool (ConnectionPool): The connection pool to store the connections.
        """

        server_socket = ServerSocket(host, port)

        server_socket.main(connection_pool)

def poller(data, connection_pool, job_queue) -> None:

    """
    A function to pool APIJob objects from the connections and add them to the job queue.
    """

    #variables
    frequency = data.get("original_frequency").get("pooler")

    #main loop
    while cycle(frequency):

        connection_pool.poll(job_queue)

def worker_process(read_end: any, connection_pool: ConnectionPool) -> None:

    """
    A function to represent the worker process (A single process that works on a stream of jobs)
    """

    while True:

        job = read_end.recv()

        check = job.job

        #decision tree here
        if isinstance(check, Signal):

            print("GOT SIGNAL to client: " + job.client_id)
            if isinstance(check, InternalInterrupt):

                break
            
            if isinstance(check, RateLimitExceeded):

                reason = Reasons.hour_rate_limit_exceeded if job.job.hour else Reasons.minute_rate_limit_exceeded if job.job.minute else Reasons.second_rate_limit_exceeded

                details = Details(States.rate_limit_exceeded, reason, None) #send instructions to the client here

                print("GOT RATE LIMIT EXCEEDED SIGNAL to client: " + job.client_id)
                connection_pool.get_client_socket(job.client_id).send(details)
            
            if False: #other signal cases

                pass

        else:

            result = job.job.call()

            connection_pool.get_client_socket(job.client_id).send(result)

class BatchWorker:

    """
    A class to represent the batch worker (A group of processes that work together)
    """

    def __init__(self, client_id: str, num_workers: int, connection_pool: ConnectionPool) -> None:

        self.client_id = client_id

        self.pool: list[Worker] = []

        self.job_count = 0

        self.connection_pool = connection_pool

        self.add_workers(num_workers)

        self.is_alive = True
    
    def add_workers(self, num_workers: int) -> None:

        for _ in range(num_workers):

            self.pool.append(Worker(self.client_id, self.connection_pool))

    def process(self, job: Job) -> None:

        self.pool[self.job_count % len(self.pool)].process(job)

        self.job_count += 1

    def cleanup(self) -> bool:

        state = False

        for worker in self.pool:

            state = worker.cleanup()

            if not state:

                break

        self.is_alive = not state

        return state
    
    def silently_interrupt(self) -> None:

        self.client_id = None

        for worker in self.pool:

            worker.silently_interrupt()

    def close_connection(self) -> None:

        stored_client_id = self.client_id

        self.silently_interrupt()

        self.connection_pool.close_client_socket(stored_client_id)

    def rename_connection(self, new_client_id: str) -> None:

        self.connection_pool.rename_client_socket(self.client_id, new_client_id)

        self.client_id = new_client_id

        for worker in self.pool:

            worker.rename_connection(new_client_id, propagate=False)

class Worker:

    """
    A class to represent the worker (A single process that works on a stream of jobs)
    """

    def __init__(self, client_id: str, connection_pool: ConnectionPool) -> None:

        self.client_id = client_id

        self.connection_pool = connection_pool

        self.write_end, read_end = Pipe()

        self.target = DillProcess(target=worker_process, args=(read_end, self.connection_pool))

        self.target.start()

        read_end.close()

        self.is_alive = True

    def process(self, job: Job) -> None:

        self.write_end.send(job)

    def cleanup(self) -> bool:

        if not self.target.is_alive():

            self.target.close()

            self.is_alive = False

            return True

        return False

    def silently_interrupt(self) -> None:

        self.client_id = None

        self.process(Job(InternalInterrupt(), self.client_id))

        self.write_end.close()

    def close_connection(self) -> None:

        stored_client_id = self.client_id

        self.silently_interrupt()

        self.connection_pool.close_client_socket(stored_client_id)

    def rename_connection(self, new_client_id: str, propagate: bool = True) -> None:

        if propagate:

            self.connection_pool.rename_client_socket(self.client_id, new_client_id)

        self.client_id = new_client_id

class WorkerPool:

    """
    A class to represent the worker pool (A group of workers may be batch workers or stream workers that work together)
    """

    def __init__(self, connection_pool: ConnectionPool) -> None:

        self.pool: list[Worker|BatchWorker] = []

        self.connection_pool = connection_pool

        self.garbage_collection_threshhold = 10

        self.garbage_collection_counter = 0

    def find_worker(self, client_id: str) -> Worker|BatchWorker:

        for worker in self.pool:

            if worker.client_id == client_id:

                return worker

        return None
    
    def allocate_worker(self, client_id: str, num_workers: int) -> None:

        if num_workers == 1:

            self.pool.append(Worker(client_id, self.connection_pool))

        else:

            self.pool.append(BatchWorker(client_id, num_workers, self.connection_pool))

    def close_connection(self, client_id: str) -> None:

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.close_connection()

    def switch_to_batch(self, num_workers: int, client_id: str) -> None:

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.silently_interrupt()

            self.allocate_worker(client_id, num_workers)

    def switch_to_stream(self, client_id: str) -> None:

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.silently_interrupt()

            self.allocate_worker(client_id, 1)

    def rename_connection(self, client_id: str, new_client_id: str) -> None:

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.rename_connection(new_client_id)

    def cleanup(self) -> None:

        to_remove = []

        for index, worker in enumerate(self.pool):

            worker.cleanup()

            if not worker.is_alive:

                to_remove.append(index)

        for index in to_remove:

            self.pool.pop(index)

    def process(self, job: Job) -> None:

        worker = self.find_worker(job.client_id)

        if worker is not None:

            worker.process(job)

    def handle_dispatch(self, job: Job) -> None:

        check = job.job

        if isinstance(check, Signal): #the object is of type signal

            if isinstance(check, Close): #the signal is of type close

                self.close_connection(job.client_id)

            elif isinstance(check, SwitchToBatch):

                self.switch_to_batch(check.num_processes, job.client_id)

            elif isinstance(check, SwitchToStream):

                self.switch_to_stream(job.client_id)

            elif isinstance(check, Rename):

                self.rename_connection(job.client_id, check.new_name)

            elif isinstance(check, RateLimitExceeded):

                self.process(job)

        else:

            self.process(job)

    def handle(self, job: Job) -> None:

        if self.garbage_collection_counter >= self.garbage_collection_threshhold:

            self.cleanup()

            self.garbage_collection_counter = 0

        else:

            self.garbage_collection_counter += 1

        self.handle_dispatch(job) 

        print([worker.__dict__ for worker in self.pool])

class Dispatcher:

    def __init__(self, data: dict, worker_pool: WorkerPool, job_queue: Queue) -> None:

        self.default, self.current, self.last_reset, self.keys = {}, {}, {}, []

        rate_limit_data = data.get("rate_limit")

        for key in rate_limit_data.keys():

            self.default[key] = rate_limit_data[key]["default"]

            self.current[key] = rate_limit_data[key]["current"]

            self.last_reset[key] = rate_limit_data[key]["last_reset"]

            self.keys.append(key)

        self.history: dict[str, list[str|float]] = {}

        self.current_job: Job = None

        self.current_key: str = None

        self.current_exceed_status: any = None

        self.worker_pool: WorkerPool = worker_pool

        self.job_queue: JobQueue= job_queue

        self.frequency: float = data.get("original_frequency").get("dispatcher")

    def allocate(self, client_id: str) -> None:

        self.history[client_id] = [None, 0]
    
    def deallocate(self, client_id: str) -> None:

        self.history.pop(client_id)

    def rename(self, client_id: str, new_client_id: str) -> None:

        self.history[new_client_id] = self.history.pop(client_id)

    def set_current_job(self, job: Job) -> None:

        self.current_job = job

    def set_rate_limit_key(self) -> None:

        for key in self.keys:

            if key in self.current_job.url:

                self.current_key = key

                return

        raise RateLimitsNotDefined(self.current_job.url)
    
    def is_rate_limit_reached(self) -> None:

        second_reached = self.current[self.current_key][0] == 0

        minute_reached = self.current[self.current_key][1] == 0

        hour_reached = self.current[self.current_key][2] == 0

        if second_reached or minute_reached or hour_reached:

            self.current_exceed_status = (second_reached, minute_reached, hour_reached)

        else:

            self.current_exceed_status = False

    def refresh_rate_limit(self) -> None:

        _last_reset = self.last_reset[self.current_key]

        if time() - _last_reset[0] >= 1:

            self.current[self.current_key][0] = self.default[self.current_key][0]

            _last_reset[0] = time()
        
        if time() - _last_reset[1] >= 60:

            self.current[self.current_key][1] = self.default[self.current_key][1]

            _last_reset[1] = time()
        
        if time() - _last_reset[2] >= 3600:

            self.current[self.current_key][2] = self.default[self.current_key][2]

            _last_reset[2] = time()

    def proceed(self) -> None:

        self.current[self.current_key][0] -= 1

        self.current[self.current_key][1] -= 1

        self.current[self.current_key][2] -= 1

    def handle_rate_limit_exceeded(self) -> None:

        condition = False

        second_exceed = self.current_exceed_status[0]

        minute_exceed = self.current_exceed_status[1]

        hour_exceed = self.current_exceed_status[2]

        if second_exceed:

            print("second")
            condition = self.history[self.current_job.client_id][0] == "second" and time() - self.history[self.current_job.client_id][1] <= 1   

        if minute_exceed:

            print("minute")
            condition = self.history[self.current_job.client_id][0] == "minute" and time() - self.history[self.current_job.client_id][1] <= 60 

        if hour_exceed:

            print("hour")
            condition = self.history[self.current_job.client_id][0] == "hour" and time() - self.history[self.current_job.client_id][1] <= 3600

        if not condition:

            #set the last time a rate limit warning was sent to the client
            self.history[self.current_job.client_id][1] = time()

            self.history[self.current_job.client_id][0] = "hour" if hour_exceed else "minute" if minute_exceed else "second"

            print(self.history[self.current_job.client_id])

            self.worker_pool.handle(Job(RateLimitExceeded(self.current_exceed_status), self.current_job.client_id))

    def handle_job(self, job: Job) -> None:

        self.set_current_job(job)

        self.set_rate_limit_key()

        self.refresh_rate_limit()

        self.is_rate_limit_reached()

        if isinstance(self.current_job.job, Signal):

            if isinstance(self.current_job.job, Close):

                self.deallocate(self.current_job.client_id)
            
            if isinstance(self.current_job.job, Rename):

                self.rename(self.current_job.client_id, self.current_job.job.new_name)

            self.worker_pool.handle(self.current_job)

        else:

            if not self.current_exceed_status:
                
                self.proceed()

                self.worker_pool.handle(self.current_job)

            else:

                self.job_queue.put_for_cycle(self.current_job)

                self.handle_rate_limit_exceeded()

    def main(self) -> None:

        while cycle(self.frequency):

            try:

                job: Job = self.job_queue.get(block=False)

            #print(job)

            except JobQueueEmpty:

                continue
            
            #allocate a process to the client if it is not already allocated (default is STREAM mode)
            if self.worker_pool.find_worker(job.client_id) is None:

                self.worker_pool.allocate_worker(job.client_id, 1)

                if job.client_id not in self.history:

                    self.allocate(job.client_id)

            self.handle_job(job)

#TODO: Implement rate limiting and statistics
def dispatcher(data, job_queue, connection_pool) -> None:

    """
    A function to dispatch the jobs to the respective processes.
    """
    #variables
    
    #of the type {client_id: process}
    process_pool: dict[int, DillProcess] = {}

    #of the type {client_id: pipe write_end}
    pipe_pool = {}

    #of the type {client_id: job_count}
    job_count = {}

    #contains the processes that have been closed via a close packet and need to be closed for the future
    to_close, garbage_counter, garbage_collection_threshhold = [], 0, 10

    worker_pool = WorkerPool(connection_pool)

    #the list to detect hits and misses to the queue and to adjust frequency dynamically
    frequency = data.get("original_frequency").get("dispatcher")
    
    #helper functions
    
    #helper function to get rate limit
    def get_rate_limit(rate_limit):

        default, current, last_reset = {}, {}, {}

        for key in rate_limit.keys():

            default[key] = rate_limit[key]["default"]

            current[key] = rate_limit[key]["current"]

            last_reset[key] = rate_limit[key]["last_reset"]

        return default, current, last_reset
    
    #helper function to get the key of the rate limit for the given url
    def get_rate_limit_key(url, rate_dict):

        for key in rate_dict.keys():

            if key in url:

                return key
            
        return None
    
    #helper function to refresh the rate limit (per second, per minute, per hour) based on the last reset time for a particular key
    def refresh_rate_limit(default, current, last_reset, key):

        _last_reset = last_reset[key]

        if time() - _last_reset[0] >= 1:

            current[key][0] = default[key][0]

            _last_reset[0] = time()
        
        if time() - _last_reset[1] >= 60:

            current[key][1] = default[key][1]

            _last_reset[1] = time()
        
        if time() - _last_reset[2] >= 3600:

            current[key][2] = default[key][2]

            _last_reset[2] = time()
    
    #helper function to check if the rate limit has been reached
    def is_rate_limit_reached(current, key):

        return current[key][0] == 0 or current[key][1] == 0 or current[key][2] == 0, [current[key][0]==0, current[key][1]==0, current[key][2]==0]
    
    #helper function to simulate a rate limit use
    def process(current, key):

        current[key][0] -= 1

        current[key][1] -= 1

        current[key][2] -= 1

    #helper function to cleanup the process pool
    def cleanup_process_pool(counter, threshold, to_close) -> int:

        #check if the garbage collection threshold has been reached
        if counter >= threshold:

            #collect the garbage processes by calling .close() on them
            new_to_close = []

            for process in to_close:

                if not process.is_alive():

                    process.close()

                else:

                    new_to_close.append(process)

            to_close = new_to_close

            return 0, to_close

        return counter+1, to_close

    #helper function to allocate a process to a client
    def allocate_process(client_id, sent_rate_limit_warning, last_rate_limit_warning):

        if sent_rate_limit_warning is not None:

            sent_rate_limit_warning[client_id] = 0

        if last_rate_limit_warning is not None:

            last_rate_limit_warning[client_id] = None

    #helper function to handle the close signal
    def handle_close(client_id, process_pool, pipe_pool, to_close, job_count, sent_rate_limit_warning, last_rate_limit_warning):

        #send the signal to the respective process(es) through the write_end(s) of the pipe connected to it and close the write_end(s)

        if isinstance(pipe_pool[client_id], list):

            for write_end in pipe_pool[client_id]:

                write_end.send(Job(Close(), client_id))

                write_end.close()

        else:

            write_end = pipe_pool[client_id]

            write_end.send(Job(Close(), client_id))

            write_end.close()

        #add the process(es) to the to_close list
        if isinstance(process_pool[client_id], list):

            for process in process_pool[client_id]:

                to_close.append(process)

        else:

            to_close.append(process_pool[client_id])

        #remove the process(es) from the process pool
        process_pool.pop(client_id)

        pipe_pool.pop(client_id)

        job_count.pop(client_id)

        sent_rate_limit_warning.pop(client_id)

        last_rate_limit_warning.pop(client_id)

    #helper function to silently interrupt the process(es) and kill them
    def silently_interrupt(client_id, process_pool, pipe_pool, to_close):

        #send the signal to the respective process(es) through the write_end(s) of the pipe connected to it and close the write_end(s)

        if isinstance(pipe_pool[client_id], list):

            for write_end in pipe_pool[client_id]:

                write_end.send(Job(InternalInterrupt(), client_id))

                write_end.close()

        else:

            write_end = pipe_pool[client_id]

            write_end.send(Job(InternalInterrupt(), client_id))

            write_end.close()

        #add the process(es) to the to_close list
        if isinstance(process_pool[client_id], list):

            for process in process_pool[client_id]:

                to_close.append(process)

        else:

            to_close.append(process_pool[client_id])

        #remove the process(es) from the process pool
        process_pool.pop(client_id)

        pipe_pool.pop(client_id)

    #helper function to handle the switch to batch signal
    def handle_switch_to_batch(num_processes, client_id, process_pool, pipe_pool, job_count):

        #what we do here is we close the current process related to the client_id in the process_pool and then allocate a new set of processes to the client
        print(process_pool[client_id])
        #close the current process(es) and deallocate the pipe end(s)
        silently_interrupt(client_id, process_pool, pipe_pool, to_close)

        #allocate a new set of processes to the client
        allocate_process(num_processes, client_id, process_pool, pipe_pool, job_count, None, None)

        print(process_pool[client_id])
    
    #helper function to handle the switch to stream signal
    def handle_switch_to_stream(client_id, process_pool, pipe_pool, job_count):

        #what we do here is we close the current process(es) related to the client_id in the process_pool and then allocate a new process to the client
        print(process_pool[client_id])
        #close the current process(es) and deallocate the pipe end(s)
        silently_interrupt(client_id, process_pool, pipe_pool, to_close)

        #allocate a new process to the client
        allocate_process(1, client_id, process_pool, pipe_pool, job_count, None, None)

        print(process_pool[client_id])
    
    #helper function to handle the rename signal
    def handle_rename(new_name, client_id, process_pool, pipe_pool, job_count, sent_rate_limit_warning, last_rate_limit_warning):

        #what we do here is to just rename the client_id in the process_pool, pipe_pool, connection_pool and job_count

        #send a rename signal to the process
        write_end = pipe_pool[client_id]

        if isinstance(write_end, list):

            write_end[0].send(Job(Rename(new_name), client_id))

        else:

            write_end.send(Job(Rename(new_name), client_id))

        if isinstance(process_pool[client_id], list):

            current = len(process_pool[client_id])

        else:

            current = 1

        silently_interrupt(client_id, process_pool, pipe_pool, to_close)

        allocate_process(current, new_name, process_pool, pipe_pool, job_count, None, None)

        job_count[new_name] = job_count.pop(client_id)

        sent_rate_limit_warning[new_name] = sent_rate_limit_warning.pop(client_id)

        last_rate_limit_warning[new_name] = last_rate_limit_warning.pop(client_id)
    
    #helper function to handle the rate limit exceeded signal
    def handle_rate_limit_exceeded(worker_pool, client_id, pipe_pool, sent_rate_limit_warning, exceed_status, last_rate_limit_warning):

        condition = False

        second_exceed = exceed_status[0]

        minute_exceed = exceed_status[1]

        hour_exceed = exceed_status[2]

        if second_exceed:

            print("second")
            condition = last_rate_limit_warning[client_id] == "second" and time() - sent_rate_limit_warning[client_id] <= 1   

        if minute_exceed:

            print("minute")
            condition = last_rate_limit_warning[client_id] == "minute" and time() - sent_rate_limit_warning[client_id] <= 60 

        if hour_exceed:

            print("hour")
            condition = last_rate_limit_warning[client_id] == "hour" and time() - sent_rate_limit_warning[client_id] <= 3600

        if not condition:

            #set the last time a rate limit warning was sent to the client
            sent_rate_limit_warning[client_id] = time()

            last_rate_limit_warning[client_id] = "hour" if hour_exceed else "minute" if minute_exceed else "second"

            print(sent_rate_limit_warning, last_rate_limit_warning)

            worker_pool.handle(Job(RateLimitExceeded(exceed_status), client_id))
            """#send the rate limit exceeded signal to the worker
            write_end = pipe_pool[client_id]

            if isinstance(write_end, list):

                write_end[0].send(Job(RateLimitExceeded(exceed_status), client_id))

            else:

                write_end.send(Job(RateLimitExceeded(exceed_status), client_id))"""
        
    #rate limits

    rate_limit = data.get("rate_limit")

    default, current, last_reset = get_rate_limit(rate_limit)

    #sent_rate_limit_warning is a dictionary to store the last time a rate limit warning was sent to the client (only once per client)
    sent_rate_limit_warning = {} #of the type {client_id: last_sent_time}

    last_rate_limit_warning = {} #of the type {client_id: last_sent_type}
    
    #print(default, current, last_reset, sent_rate_limit_warning)
    #helper function to handle the job
    def handle_job(worker_pool, job, client_id, pipe_pool, sent_rate_limit_warning, last_rate_limit_warning):

        if isinstance(job.job, Signal): #the object is of type signal
            """
                if isinstance(job.job, Close): #the signal is of type close

                    handle_close(client_id, process_pool, pipe_pool, to_close, job_count, sent_rate_limit_warning, last_rate_limit_warning)

                elif isinstance(job.job, SwitchToBatch):

                    handle_switch_to_batch(job.job.num_processes, client_id, process_pool, pipe_pool, job_count)

                elif isinstance(job.job, SwitchToStream):

                    handle_switch_to_stream(client_id, process_pool, pipe_pool, job_count)

                elif isinstance(job.job, Rename):

                    handle_rename(job.job.new_name, client_id, process_pool, pipe_pool, job_count, sent_rate_limit_warning, last_rate_limit_warning)"""
            worker_pool.handle(job)

        else: #the object is of type job

            #get the rate limit key
            rate_limit_key = get_rate_limit_key(job.job.url, rate_limit)

            if rate_limit_key is None:

                raise NotImplementedError("Rate limit not implemented for the given url")
            
            else:

                #refresh the rate limit
                refresh_rate_limit(default, current, last_reset, rate_limit_key)

                #check if the rate limit has been reached
                check, exceed_status = is_rate_limit_reached(current, rate_limit_key)

                if check:

                    #rate limit has been reached so we put the job back into the job queue for cycling purposes
                    job_queue.put_for_cycle(job)

                    handle_rate_limit_exceeded(worker_pool, client_id, pipe_pool, sent_rate_limit_warning, exceed_status, last_rate_limit_warning)


                else:

                    #process the rate limit
                    process(current, rate_limit_key)

                    #send the job
                    worker_pool.handle(job)
                    """write_end = pipe_pool[client_id]

                    if isinstance(write_end, list):

                        write_end[job_count[client_id]%len(write_end)].send(job)

                    else:

                        write_end.send(job)

                    job_count[client_id] += 1"""

    #main loop
    while cycle(frequency):

        garbage_counter, to_close = cleanup_process_pool(garbage_counter, garbage_collection_threshhold, to_close)

        try:

            job: Job = job_queue.get(block=False)

            #print(job)

        except JobQueueEmpty:

            continue
            
        #allocate a process to the client if it is not already allocated (default is STREAM mode)
        if worker_pool.find_worker(job.client_id) is None:

            worker_pool.allocate_worker(job.client_id, 1)
            """client_id = job.client_id

            if client_id not in process_pool:"""

            allocate_process(job.client_id, sent_rate_limit_warning, last_rate_limit_warning)

        #handle the job
        handle_job(worker_pool, job, job.client_id, pipe_pool, sent_rate_limit_warning, last_rate_limit_warning)

        #print(last_rate_limit_warning, sent_rate_limit_warning)
            
def worker(read_end, broadcast_pool) -> None:

    """
    A worker process to process the jobs
    """

    while True:

        job = read_end.recv()

        if isinstance(job.job, Signal):

            if isinstance(job.job, Close):

                packed = Status(Details(States.closed_connection, Reasons.client_sent_close_packet, None), job.client_id)

                broadcast_pool.put(packed)

                break

            if isinstance(job.job, InternalInterrupt):

                break
            
            if isinstance(job.job, Rename):

                packed = Status(Details(States.renamed_connection, Reasons.client_sent_rename_packet, [job.job.new_name]), job.client_id)

                broadcast_pool.put(packed)

                break

            if isinstance(job.job, RateLimitExceeded):

                reason = Reasons.hour_rate_limit_exceeded if job.job.hour else Reasons.minute_rate_limit_exceeded if job.job.minute else Reasons.second_rate_limit_exceeded

                packed = Status(Details(States.rate_limit_exceeded, reason, None), job.client_id) #send instructions to the client here

                broadcast_pool.put(packed)

            
        else:

            #TODO: implement error handling
            result = job.job.call()
         
            packed = Result(result, job.client_id)

            broadcast_pool.put(packed)

def broadcaster(broadcast_pool, connection_pool: ConnectionPool) -> None:

    """
    A function to broadcast the results to the respective clients.
    """

    while True:

        try:

            data = broadcast_pool.get()

        except BroadcastQueueEmpty:

            continue

        #check if object is of type Status
        if isinstance(data, Status):

            #check if the state is closed_connection
            if data.details.state == States.closed_connection:

                connection_pool.close_client_socket(data.client_id)

                print(connection_pool.get_pool())

            elif data.details.state == States.renamed_connection:

                connection_pool.rename_client_socket(data.client_id, data.details.instruction[0])

                connection_pool.pop_client_socket(data.client_id)

                print(connection_pool.get_pool())

            elif data.details.state == States.rate_limit_exceeded:

                connection_pool.get_client_socket(data.client_id).send(data.details)

            else: #TODO: handle other statuses

                #send the status to the client
                connection_pool.get_client_socket(data.client_id).send(data.details)

        else:

            print(connection_pool.get_pool(), connection_pool.get_client_socket(data.client_id), data.client_id, data.result)
            #send the result to the client (it must be of type Result)
            connection_pool.get_client_socket(data.client_id).send(data.result)



