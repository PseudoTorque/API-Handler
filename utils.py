"""
Utility functions and classes for the project.
"""

from multiprocessing import Process, Pipe
from time import sleep
from socket import error, socket, AF_INET, SOCK_STREAM
import errno

import dill

from constants import Priority, Signal, Signals, Job, Result, Status, States, Reasons, Details, APIJob, Dummy

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

def listener(host: str, port: int, connection_pool) -> None:

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

        while True:
            
            #accept the connection and add it to the connection pool
            #assign a key of the lowest value to the connection
            conn, _ = Socket.accept()

            i = 0

            while i in connection_pool:

                i += 1

            connection_pool[i] = conn

            print(connection_pool)

def decode_object(socket: any) -> any:

    """
    A function to decode the bytes into an object.
    """

    try:

        try:

            header_size = 30

            #bytes is a string of bytes that is a pickled object.

            header = socket.recv(header_size)

            message_length = int(header.decode("utf-8").strip())

            return dill.loads(socket.recv(message_length))
        
        except error as e:
                    
            if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:

                return None

    except Exception as e:

        print(e)

    return None
        
def encode_object(data: any) -> bytes:

    """
    A function to encode the object into bytes.
    """

    header_size = 30

    object_bytes = dill.dumps(data)

    out = bytes(f"{len(object_bytes):<{header_size}}", 'utf-8') + object_bytes

    return out

def pooler(frequency_dict, connection_pool, job_queue) -> None:

    """
    A function to pool APIJob objects from the connections and add them to the job queue.
    """

    while True: #cycle(frequency_dict["pooler"]):

        for i in connection_pool.keys():

            conn = connection_pool.get(i)

            if conn is not None:

                if conn.getblocking():
                    conn.setblocking(False)

                job = decode_object(conn)

                if job is None:
                    continue

                #initialize the APIJob Object by refactoring the job object into a dictionary
                job_dict = job.__dict__

                if "signal" in job_dict:

                    job = Signal(**job.__dict__)

                else:

                    job = Dummy(**job.__dict__)

                job.priority = Priority(**job.priority.__dict__)

                print("jp: " + str(job.priority.group_priority * 10 + job.priority.internal_priority) + " cid: " + str(i))

                job_queue.put(Job(job, i))

#TODO: Implement rate limiting and statistics
def dispatcher(frequency_dict, job_queue, broadcast_pool) -> None:

    """
    A function to dispatch the jobs to the respective processes.
    """

    #of the type {client_id: process}
    process_pool: dict[int, DillProcess] = {}

    #of the type {client_id: connection}
    pipe_pool = {}

    #contains the processes that have been closed via a close packet and need to be closed for the future
    to_close, garbage_counter, garbage_collection_threshhold = [], 0, 10

    

    while cycle(frequency_dict["dispatcher"]):

        #check if the garbage collection threshold has been reached
        if garbage_counter >= garbage_collection_threshhold:

            #collect the garbage processes by calling .close() on them
            new_to_close = []

            for process in to_close:

                if not process.is_alive():

                    process.close()

                    print("closed process.")

                else:

                    new_to_close.append(process)

            to_close = new_to_close
            
            print("got new garbage" + str(to_close))
            garbage_counter = 0

        #get a job from the job_queue
        job: Job = job_queue.get()
        print("got job")

        client_id = job.client_id

        if client_id not in process_pool:

            #create a new pipe for datatransfer
            read_end, write_end = Pipe()

            #pass the read_end to the proces.
            target = DillProcess(target=worker, args=(read_end, broadcast_pool))

            #add process to the process pool
            process_pool[client_id] = target

            #start the process
            target.start()
            
            #close the read_end
            read_end.close()

            #append the write end to the pipe pool
            pipe_pool[client_id] = write_end

        #send the job
        print("jp: " + str(job.job.priority.group_priority * 10 + job.job.priority.internal_priority) + " cid: " + str(client_id))
        write_end = pipe_pool[client_id]

        write_end.send(job)
        
        if isinstance(job.job, Signal): #TODO: check if the signal is of type close
            #send the signal to the respective process through the write_end of the pipe connected to it
            
            #add the process to the to_close list
            to_close.append(process_pool[client_id])

            print(to_close)
            #remove the process from the process pool
            process_pool.pop(client_id)

            #close the write_end of the pipe and remove it from the pipe_pool
            write_end = pipe_pool[client_id]

            write_end.close()

            pipe_pool.pop(client_id)

        garbage_counter += 1

def worker(read_end, broadcast_pool) -> None:

    """
    A worker process to process the jobs
    """

    while True:

        job = read_end.recv()

        if isinstance(job.job, Signal):

            if job.job.signal == Signals.close:

                packed = Status(Details(States.closed_connection, Reasons.client_sent_close_packet, None), job.client_id)

                broadcast_pool.put(packed)


                break

        else:

            #TODO: implement error handling
            result = job.job.call()
         
            packed = Result(result, job.client_id)

            broadcast_pool.put(packed)

#TODO: want to send status after close???
def broadcaster(broadcast_pool, connection_pool) -> None:

    """
    A function to broadcast the results to the respective clients.
    """

    while True:

        data = broadcast_pool.get()

        #check if object is of type Status
        if isinstance(data, Status):

            #check if the state is closed_connection
            if data.details.state == States.closed_connection:
                
                #send the close packet to the client
                connection_pool[data.client_id].send(encode_object(data.details))

                #remove the connection from the connection pool
                connection_pool.pop(data.client_id)
                print(connection_pool)

        else:

            #send the result to the client (it must be of type Result)
            connection_pool[data.client_id].send(encode_object(data.result))
