"""
Utility functions and classes for the project.
"""

from multiprocessing import Process, Pipe
from time import sleep, time
from socket import error, socket, AF_INET, SOCK_STREAM
import errno

import dill

from exceptions import JobQueueEmpty, RateLimitsNotDefined #exceptions

from objects import Close, SwitchToBatch, SwitchToStream, Rename, InternalInterrupt, RateLimitExceeded, Signal #signals
from objects import Priority, APIJob, Dummy, Job #job related objects
from objects import States, Reasons, Details #states, reasons and details

def reformat_object(obj: any) -> any:

    """
    A function to reformat the object into the appropriate object, we need to do this
    as local class instances are not pickled right and therefore have to be recast.

    Args:
        obj (any): The object to reformat.

    Returns:
        any: The reformatted object.
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
    Assumes that the object has to be recieved from the
    provided socket in a non-blocking manner.

    Args:
        sock (socket): The socket to decode the object from.

    Returns:
        any: The decoded object.
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

    Args:
        data (any): The object to encode.

    Returns:
        bytes: The encoded object.
    """

    header_size = 30

    object_bytes = dill.dumps(data)

    out = bytes(f"{len(object_bytes):<{header_size}}", 'utf-8') + object_bytes

    return out

#call a sort function on the job queue after rate limits have been available again
class JobQueue:

    """
    A class to represent the job queue, the job queue is a list of jobs that are sorted by priority, 
    some functions may override this behaviour.
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

    def sort(self) -> None:

        self.buffer = sorted(self.buffer, reverse=True)

class ClientSocket:

    """
    A class to represent a client socket.
    """

    def __init__(self, connection: socket, client_id: str) -> None:

        """
        A function to initialize the client socket.

        Args:
            connection (socket): The connection to the client.
            client_id (str): The client id to identify the client.
        """

        self.connection: socket = connection

        self.client_id: str = client_id

    def send(self, data: any) -> None:

        """
        A function to send data through the socket.

        Args:
            data (any): The data to send through the socket.
        """

        self.connection.send(encode_object(data))

    def recv(self) -> any:

        """
        A function to receive data from the client socket.

        Returns:
            any: The data received from the client socket.
        """

        return decode_object(self.connection)

class ConnectionPool:

    """
    A class to represent the connection pool.
    """

    def __init__(self) -> None:

        """
        A function to initialize the connection pool.
        """

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

            job_queue.put(Job(Close(), client_id)) #just in case the client close was not propagated to the dispatcher (ungraceful)
            self.close_client_socket(client_id)

class ServerSocket:

    """
    A class to represent the server socket.
    """

    def __init__(self, host: str, port: int) -> None:

        """
        A function to initialize the server socket.

        Args:
            host (str): The host to listen on.
            port (int): The port to listen on.
        """

        self.server_connection: socket = socket(AF_INET, SOCK_STREAM)

        self.server_connection.bind((host, port))

        self.server_connection.listen()

    def main(self, connection_pool: ConnectionPool) -> None:

        """
        A function to listen for connections and add them to the connection pool.

        Args:
            connection_pool (ConnectionPool): The connection pool to add the connections to.
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
    A function to cycle code at a given frequency.

    Args:
        frequency (float): The frequency at which the code should cycle.

    Returns:
        bool: True if the code has cycled, False otherwise.
    """

    sleep(1/frequency)

    return True

def listener(host: str, port: int, connection_pool: ConnectionPool) -> None:

    """
    A function to listen for connections and add them to the connection pool.

    Args:
        host (str): The host to listen on.
        port (int): The port to listen on.
        connection_pool (ConnectionPool): The connection pool to store the connections.
    """

    server_socket = ServerSocket(host, port)

    server_socket.main(connection_pool)

def poller(data: dict[str, any], connection_pool: ConnectionPool, job_queue: JobQueue) -> None:

    """
    A function to pool APIJob objects from the connections and add them to the job queue.

    Args:
        data (any): The data to initialize the poller with.
        connection_pool (ConnectionPool): The connection pool to poll.
        job_queue (JobQueue): The job queue to add the jobs to.
    """

    #variables
    frequency = data.get("original_frequency").get("pooler")

    #main loop
    while cycle(frequency):

        connection_pool.poll(job_queue)

def worker_process(read_end: any, connection_pool: ConnectionPool) -> None:

    """
    A function to represent the worker process (A single process that works on a stream of jobs)

    Args:
        read_end (any): The read end of the pipe to receive jobs from.
        connection_pool (ConnectionPool): The connection pool to send results to/modify.
    """

    while True:

        try:

            job = read_end.recv()

        except EOFError:

            break

        check = job.job

        #decision tree here
        if isinstance(check, Signal):

            if isinstance(check, InternalInterrupt):

                break
            
            if isinstance(check, RateLimitExceeded):

                reason = Reasons.hour_rate_limit_exceeded if job.job.hour else Reasons.minute_rate_limit_exceeded if job.job.minute else Reasons.second_rate_limit_exceeded

                details = Details(States.rate_limit_exceeded, reason, None) #send instructions to the client here

                connection_pool.get_client_socket(job.client_id).send(details)
      
            #other signal cases here

        else:



            result = job.job.call()

            connection_pool.get_client_socket(job.client_id).send(result)

            print(result)

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

        """
        A function to add workers of type Worker to the batch worker.

        Args:
            num_workers (int): The number of workers to add.
        """

        for _ in range(num_workers):

            self.pool.append(Worker(self.client_id, self.connection_pool))

    def process(self, job: Job) -> None:

        """
        A function to dispatch the job to some worker in the batch worker.

        Args:
            job (Job): The job to process.
        """

        self.pool[self.job_count % len(self.pool)].process(job)

        self.job_count += 1

    def cleanup(self) -> bool:

        """
        A function to cleanup the batch worker, checks if all workers are alive.

        Returns:
            bool: True if all workers have been cleaned up, False otherwise.
        """

        state = False

        for worker in self.pool:

            state = worker.cleanup()

            if not state:

                break

        self.is_alive = not state

        return state

    def silently_interrupt(self) -> None:

        """
        A function to silently interrupt the batch worker, sets the client id to None and silently interrupts all workers.
        """

        self.client_id = None

        for worker in self.pool:

            worker.silently_interrupt()

    def close_connection(self) -> None:

        """
        A function to close the batch worker, sets the client id to None and 
        silently interrupts all workers, relinquishes the client id to the 
        connection pool and frees resources.
        """

        stored_client_id = self.client_id

        self.silently_interrupt()

        self.connection_pool.close_client_socket(stored_client_id)

    def rename_connection(self, new_client_id: str) -> None:

        """
        A function to rename the batch worker, renames the client id to the new 
        client id and propagates the change to the connection pool.

        Args:
            new_client_id (str): The new client id to rename to.
        """

        self.connection_pool.rename_client_socket(self.client_id, new_client_id)

        self.client_id = new_client_id

        for worker in self.pool:

            worker.rename_connection(new_client_id, propagate=False)

class Worker:

    """
    A class to represent the worker (A single process that works on a stream of jobs)
    """

    def __init__(self, client_id: str, connection_pool: ConnectionPool) -> None:

        """
        A function to initialize the worker object.

        Args:
            client_id (str): The client id to initialize the worker with.
            connection_pool (ConnectionPool): The connection pool to initialize the worker with.
        """

        self.client_id = client_id

        self.connection_pool = connection_pool

        self.write_end, read_end = Pipe()

        self.target = DillProcess(target=worker_process, args=(read_end, self.connection_pool))

        self.target.start()

        read_end.close()

        self.is_alive = True

    def process(self, job: Job) -> None:

        """
        A function to dispatch the job to the worker, sends the job to the worker's target
        through the write end of the pipe associated with it.

        Args:
            job (Job): The job to dispatch.
        """

        self.write_end.send(job)

    def cleanup(self) -> bool:

        """
        A function to cleanup the worker, checks if the worker is alive and if the target is alive.

        Returns:
            bool: True if the worker has been cleaned up, False otherwise.
        """

        print("cleanup")
        if self.is_alive:

            if not self.target.is_alive():

                print(self.target, self.target.is_alive())
                self.target.close()

                self.is_alive = False

                return True

            return False

        else:

            return True

    def silently_interrupt(self) -> None:

        """
        A function to silently interrupt the worker, sets the client id to None and silently interrupts the worker.
        """

        self.client_id = None

        self.process(Job(InternalInterrupt(), self.client_id))

        self.write_end.close()

    def close_connection(self) -> None:

        """
        A function to close the worker, sets the client id to None and silently interrupts the worker, 
        relinquishes the client id to the connection pool and frees resources.
        """

        stored_client_id = self.client_id

        self.silently_interrupt()

        self.connection_pool.close_client_socket(stored_client_id)

    def rename_connection(self, new_client_id: str, propagate: bool = True) -> None:

        """
        A function to rename the worker, renames the client id to the new client id and propagates the change to the connection pool.

        Args:
            new_client_id (str): The new client id to rename to.
            propagate (bool): Whether to propagate the change to the connection pool.
        """

        if propagate:

            self.connection_pool.rename_client_socket(self.client_id, new_client_id)

        self.client_id = new_client_id

class WorkerPool:

    """
    A class to represent the worker pool (A group of workers may 
    be a batch worker or stream worker that work together)
    """

    def __init__(self, connection_pool: ConnectionPool) -> None:

        """
        A function to initialize the worker pool object.

        Args:
            connection_pool (ConnectionPool): The connection pool to initialize the worker pool with.
        """

        self.pool: list[Worker|BatchWorker] = []

        self.connection_pool = connection_pool

        self.garbage_collection_threshhold = 10

        self.garbage_collection_counter = 0

    def find_worker(self, client_id: str) -> Worker|BatchWorker|None:

        """
        A function to find a worker in the worker pool.

        Args:
            client_id (str): The client id to find the worker with.

        Returns:
            Worker|BatchWorker|None: The worker or batch worker that 
            matches the client id or None if no worker is found.
        """

        for worker in self.pool:

            if worker.client_id == client_id:

                return worker

        return None

    def allocate_worker(self, client_id: str, num_workers: int) -> None:

        """
        A function to allocate a worker to the worker pool.

        Args:
            client_id (str): The client id to allocate the worker to.
            num_workers (int): The number of workers to allocate.
        """

        if num_workers == 1:

            self.pool.append(Worker(client_id, self.connection_pool))

        else:

            self.pool.append(BatchWorker(client_id, num_workers, self.connection_pool))

    def close_connection(self, client_id: str) -> None:

        """
        A function to close the connection of a worker in the worker pool,
        also propagates the change to the connection pool.

        Args:
            client_id (str): The client id to close the connection of.
        """

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.close_connection()

    def switch_to_batch(self, num_workers: int, client_id: str) -> None:

        """
        A function to switch the worker to a batch worker, silently interrupts the worker and 
        allocates a new batch worker to the worker pool.

        Args:
            num_workers (int): The number of workers to allocate.
            client_id (str): The client id to switch the worker to.
        """

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.silently_interrupt()

            self.allocate_worker(client_id, num_workers)

    def switch_to_stream(self, client_id: str) -> None:

        """
        A function to switch the worker to a stream worker, silently interrupts the worker and 
        allocates a new stream worker to the worker pool.

        Args:
            client_id (str): The client id to switch the worker to.
        """

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.silently_interrupt()

            self.allocate_worker(client_id, 1)

    def rename_connection(self, client_id: str, new_client_id: str) -> None:

        """
        A function to rename the connection of a worker in the worker pool, 
        renames the client id to the new client id.

        Args:
            client_id (str): The client id to rename the connection of.
            new_client_id (str): The new client id to rename to.
        """

        worker = self.find_worker(client_id)

        if worker is not None:

            worker.rename_connection(new_client_id)

    def cleanup(self) -> None:

        """
        A function to cleanup the worker pool, checks if all workers are alive 
        and if they have been cleaned up appropriately, removes the workers that 
        have been cleaned up fully, from the worker pool.
        """

        to_remove = []

        for index, worker in enumerate(self.pool):

            worker.cleanup()

            if not worker.is_alive:

                to_remove.append(index)

        self.pool = [worker for index, worker in enumerate(self.pool) if index not in to_remove]

    def process(self, job: Job) -> None:

        """
        A function to process the job, finds the worker associated with the job's client id and 
        processes the job through the worker.

        Args:
            job (Job): The job to process.
        """

        worker = self.find_worker(job.client_id)

        if worker is not None:

            worker.process(job)

    def cleanup_cycle(self) -> None:

        """
        A function to cleanup the worker pool, checks if the garbage collection 
        counter has reached the garbage collection threshhold and if so, 
        calls the cleanup function.
        """

        if self.garbage_collection_counter >= self.garbage_collection_threshhold:

            self.cleanup()

            self.garbage_collection_counter = 0

        else:

            self.garbage_collection_counter += 1

        print([worker.__dict__ for worker in self.pool])

class Dispatcher:

    """
    A class to represent the dispatcher (A single process that handles the jobs)
    """

    def __init__(self, data: dict, worker_pool: WorkerPool, job_queue: JobQueue) -> None:

        """
        A function to initialize the dispatcher object.

        Args:
            data (dict): The data to initialize the dispatcher with.
            worker_pool (WorkerPool): The worker pool to initialize the dispatcher with.
            job_queue (JobQueue): The job queue to initialize the dispatcher with.
        """

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

        self.job_count = 0

    def allocate(self, client_id: str) -> None:

        """
        A function to allocate a client in the dispatcher data (history (rate limit history)).

        Args:
            client_id (str): The client id to allocate.
        """

        self.history[client_id] = [None, 0]

    def deallocate(self, client_id: str) -> None:

        """
        A function to deallocate a client in the dispatcher data (history (rate limit history)).

        Args:
            client_id (str): The client id to deallocate.
        """

        self.history.pop(client_id)

    def rename(self, client_id: str, new_client_id: str) -> None:

        """
        A function to rename a client in the dispatcher data (history (rate limit history)).

        Args:
            client_id (str): The client id to rename.
            new_client_id (str): The new client id to rename to.
        """

        self.history[new_client_id] = self.history.pop(client_id)

    def set_current_job(self, job: Job) -> None:

        """
        A function to set the current job.

        Args:
            job (Job): The job to set the current job to.
        """

        self.current_job = job

    def set_rate_limit_key(self) -> None:

        """
        A function to set the current rate limit key, from the rate limit data for specific 
        urls, compares the url of the current job with the urls in the rate limit data.

        Raises:
            RateLimitsNotDefined: If the rate limit data is not defined for the current job url.
        """

        for key in self.keys:

            if key in self.current_job.job.url:

                self.current_key = key

                return

        raise RateLimitsNotDefined(self.current_job.url)

    def is_rate_limit_reached(self) -> None:

        """
        A function to check if the rate limit for the current job url 
        is reached and set the current exceed status if so.
        """

        second_reached = self.current[self.current_key][0] == 0

        minute_reached = self.current[self.current_key][1] == 0

        hour_reached = self.current[self.current_key][2] == 0

        if second_reached or minute_reached or hour_reached:

            self.current_exceed_status = (second_reached, minute_reached, hour_reached)

        else:

            self.current_exceed_status = False

    def refresh_rate_limit(self) -> None:

        """
        A function to refresh the rate limit for the current job url.
        """

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

        """
        A function to proceed the rate limit for the current job url (decrement the rate limit counters).
        """

        self.current[self.current_key][0] -= 1

        self.current[self.current_key][1] -= 1

        self.current[self.current_key][2] -= 1

    def handle_rate_limit_exceeded(self) -> None:

        """
        A function to handle if the rate limit for the current job url is exceeded.
        """

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

            self.worker_pool.process(Job(RateLimitExceeded(self.current_exceed_status), self.current_job.client_id))

    def handle_job(self, job: Job) -> None:

        """
        A function to handle the job.

        Args:
            job (Job): The job to handle.
        """

        self.set_current_job(job)

        check = self.current_job.job

        client_id = self.current_job.client_id

        if isinstance(check, Signal):

            if isinstance(check, Close):

                self.worker_pool.close_connection(client_id)

                self.deallocate(client_id)

            elif isinstance(check, SwitchToBatch):

                self.worker_pool.switch_to_batch(check.num_processes, client_id)

            elif isinstance(check, SwitchToStream):

                self.worker_pool.switch_to_stream(client_id)
            
            elif isinstance(check, Rename):

                self.worker_pool.rename_connection(client_id, check.new_name)

                self.rename(client_id, check.new_name)

        else:

            prior = self.current_exceed_status

            self.set_rate_limit_key()

            self.refresh_rate_limit()

            self.is_rate_limit_reached()

            print(self.current_exceed_status)

            if not self.current_exceed_status:
                
                if prior:

                    self.job_queue.sort()

                self.proceed()

                print("proceeding")
                self.worker_pool.process(self.current_job)

            else:

                self.job_queue.put_for_cycle(self.current_job)

                self.handle_rate_limit_exceeded()

        self.worker_pool.cleanup_cycle()

    def main(self) -> None:

        """
        A function for the main loop of the dispatcher.
        """

        while cycle(self.frequency):

            try:

                job: Job = self.job_queue.get(block=False)

            except JobQueueEmpty:

                continue
            
            #allocate a process to the client if it is not already allocated (default is STREAM mode)
            if self.worker_pool.find_worker(job.client_id) is None:

                self.worker_pool.allocate_worker(job.client_id, 1)

                if job.client_id not in self.history:

                    self.allocate(job.client_id)

            print("got job", job.job.__dict__.get('priority').__dict__)

            self.handle_job(job)

            print("handled job")

            self.job_count += 1

            print(self.current, self.current_exceed_status)

            print(self.job_queue.get_buffer())

            print(self.job_count)

def dispatcher(data, job_queue, connection_pool) -> None:

    """
    A function to dispatch the jobs to the respective processes, uses the dispatcher class.

    Args:
        data (dict): The data to initialize the dispatcher with.
        job_queue (JobQueue): The job queue to initialize the dispatcher with.
        connection_pool (ConnectionPool): The connection pool to initialize the worker pool with.
    """

    worker_pool = WorkerPool(connection_pool)

    target = Dispatcher(data, worker_pool, job_queue)

    target.main()