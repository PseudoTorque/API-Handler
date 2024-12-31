"""
Utility functions and classes for the project.
"""

from multiprocessing import Process, Pipe
from time import sleep, time
from socket import error, socket, AF_INET, SOCK_STREAM
import errno
from queue import Empty

import dill

from objects import InternalInterrupt, Priority, RateLimitExceeded, Result, Status, States, Reasons, Details, APIJob, Dummy, Job, Signal, Close, SwitchToBatch, SwitchToStream, Rename

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
        sock = socket(AF_INET, SOCK_STREAM)

        sock.bind((host, port))
        
        sock.listen()

        while True:
            
            #accept the connection and add it to the connection pool
            #assign a key of the lowest value to the connection
            conn, _ = sock.accept()

            i = 0

            while i in connection_pool:

                i += 1

            connection_pool[i] = conn

            print(connection_pool)

def decode_object(sock: any) -> any:

    """
    A function to decode the bytes into an object.
    """

    try:

        header_size = 30

        #bytes is a string of bytes that is a pickled object.

        header = sock.recv(header_size)

        header = header.decode("utf-8").strip()

        if header != "":
            message_length = int(header)

            return dill.loads(sock.recv(message_length))
    
    except error as e:
                
        if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:

            pass
    
    return None

def encode_object(data: any) -> bytes:

    """
    A function to encode the object into bytes.
    """

    header_size = 30

    object_bytes = dill.dumps(data)

    out = bytes(f"{len(object_bytes):<{header_size}}", 'utf-8') + object_bytes

    return out

def pooler(statistics, connection_pool, job_queue) -> None:

    """
    A function to pool APIJob objects from the connections and add them to the job queue.
    """
    #variables
    frequency, buffer_length, buffer, min_frequency, adjusting_frequency = statistics.get("original_frequency").get("pooler"), statistics.get("buffer_length").get("pooler"), [], statistics.get("min_frequency").get("pooler"), False

    #helper functions

    #helper function to reformat the received object into a local object
    def reformat_object(obj: any) -> any:

        #initialize the APIJob Object by refactoring the job object into a dictionary
        job_dict = obj.__dict__

        if isinstance(obj, Signal):

            if isinstance(obj, Close):

                job = Close()

            elif isinstance(obj, SwitchToBatch):

                job = SwitchToBatch(job_dict["num_processes"])

            elif isinstance(obj, SwitchToStream):

                job = SwitchToStream()

            elif isinstance(obj, Rename):

                job = Rename(job_dict["new_name"])

        else:

                job = APIJob(**job_dict)

        job.priority = Priority(**job_dict["priority"].__dict__)

        return job
     #the list to detect hits and misses to the queue and to adjust frequency dynamically

    #helper function to adjust the frequency dynamically
    def adjust_frequency(state, buffer, buffer_length, frequency, min_frequency, adjusting_frequency) -> tuple[float, bool]:

        #three cases:
        #1. the buffer is full of 0s i.e misses, then we need to decrease the frequency to a predetermined value
        #2. the buffer is full of 1s i.e hits, then we need to increase the frequency to the point where there is a suboptimal hit rate <100%
        #3. the buffer is has either 0s or 1s, then we need to exponentially increase the frequency
        print(buffer, frequency, adjusting_frequency, buffer_length)
        if len(buffer) == buffer_length:

            buffer.pop(0)

            buffer.append(state)

            if all([state == 0 for state in buffer]):

                frequency *= 0.5

                if frequency < min_frequency:

                    frequency = min_frequency

            elif all([state == 1 for state in buffer]):

                frequency *= 2

                adjusting_frequency = True

            else:

                

                if not adjusting_frequency:

                    frequency *= buffer.count(1)/buffer_length

                else:

                    frequency *= 0.8

                    adjusting_frequency = False
        
        else:

            buffer.append(state)

        return frequency, adjusting_frequency
    
    #main loop
    while cycle(frequency):

        for i in connection_pool.keys():

            conn = connection_pool.get(i)

            if conn is not None:

                if conn.getblocking():
                    conn.setblocking(False)

                job = decode_object(conn)

                if job is None:

                    #frequency, adjusting_frequency = adjust_frequency(0, buffer, buffer_length, frequency, min_frequency, adjusting_frequency)

                    continue
                
                #frequency, adjusting_frequency = adjust_frequency(1, buffer, buffer_length, frequency, min_frequency, adjusting_frequency)

                job = reformat_object(job)

                print("jp: " + str(job.priority.group_priority * 10 + job.priority.internal_priority) + " cid: " + str(i))

                job_queue.put(Job(job, i))

#TODO: Implement rate limiting and statistics
def dispatcher(statistics, job_queue, broadcast_pool) -> None:

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

    #the list to detect hits and misses to the queue and to adjust frequency dynamically
    frequency, buffer_length, buffer, min_frequency, adjusting_frequency = statistics.get("original_frequency").get("dispatcher"), statistics.get("buffer_length").get("dispatcher"), [], statistics.get("min_frequency").get("dispatcher"), False
    
    #helper functions

    #helper function to adjust the frequency dynamically
    def adjust_frequency(state, buffer, buffer_length, frequency, min_frequency, adjusting_frequency) -> tuple[float, bool]:

        #three cases:
        #1. the buffer is full of 0s i.e misses, then we need to decrease the frequency to a predetermined value
        #2. the buffer is full of 1s i.e hits, then we need to increase the frequency to the point where there is a suboptimal hit rate <100%
        #3. the buffer is has either 0s or 1s, then we need to exponentially increase the frequency
        #print(buffer, frequency, adjusting_frequency, buffer_length)
        if len(buffer) == buffer_length:

            buffer.pop(0)

            buffer.append(state)

            if all([state == 0 for state in buffer]):

                frequency *= 0.5

                if frequency < min_frequency:

                    frequency = min_frequency

            elif all([state == 1 for state in buffer]):

                frequency *= 1.25

                adjusting_frequency = True

            else:

                if not adjusting_frequency:

                    frequency *= buffer.count(1)/buffer_length

                else:

                    frequency *= 0.8

                    adjusting_frequency = False
        
        else:

            buffer.append(state)

        return frequency, adjusting_frequency
    
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
    def allocate_process(num_processes, client_id, process_pool, pipe_pool, job_count, sent_rate_limit_warning, last_rate_limit_warning):

        if num_processes == 1:

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
        
        else:

            #create a list of tuples of the form (process, write_end)
            allocation = []

            for i in range(num_processes):

                read_end, write_end = Pipe()

                target = DillProcess(target=worker, args=(read_end, broadcast_pool))

                target.start()

                read_end.close()

                allocation.append((target, write_end))

            process_pool[client_id] = [i[0] for i in allocation]

            pipe_pool[client_id] = [i[1] for i in allocation]
        
        job_count[client_id] = 0

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
    def handle_rate_limit_exceeded(client_id, pipe_pool, sent_rate_limit_warning, exceed_status, last_rate_limit_warning):

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

        print(condition)
        if not condition:

            #set the last time a rate limit warning was sent to the client
            sent_rate_limit_warning[client_id] = time()

            last_rate_limit_warning[client_id] = "hour" if hour_exceed else "minute" if minute_exceed else "second"

            print(sent_rate_limit_warning, last_rate_limit_warning)
            #send the rate limit exceeded signal to the worker
            write_end = pipe_pool[client_id]

            if isinstance(write_end, list):

                write_end[0].send(Job(RateLimitExceeded(exceed_status), client_id))

            else:

                write_end.send(Job(RateLimitExceeded(exceed_status), client_id))
        
    #rate limits

    rate_limit = statistics.get("rate_limit")

    default, current, last_reset = get_rate_limit(rate_limit)

    #sent_rate_limit_warning is a dictionary to store the last time a rate limit warning was sent to the client (only once per client)
    sent_rate_limit_warning = {} #of the type {client_id: last_sent_time}

    last_rate_limit_warning = {} #of the type {client_id: last_sent_type}
    
    print(default, current, last_reset, sent_rate_limit_warning)
    #helper function to handle the job
    def handle_job(job, client_id, process_pool, pipe_pool, to_close, job_count, sent_rate_limit_warning, last_rate_limit_warning):

        if isinstance(job.job, Signal): #the object is of type signal

            if isinstance(job.job, Close): #the signal is of type close

                handle_close(client_id, process_pool, pipe_pool, to_close, job_count, sent_rate_limit_warning, last_rate_limit_warning)

            elif isinstance(job.job, SwitchToBatch):

                handle_switch_to_batch(job.job.num_processes, client_id, process_pool, pipe_pool, job_count)

            elif isinstance(job.job, SwitchToStream):

                handle_switch_to_stream(client_id, process_pool, pipe_pool, job_count)

            elif isinstance(job.job, Rename):

                handle_rename(job.job.new_name, client_id, process_pool, pipe_pool, job_count, sent_rate_limit_warning, last_rate_limit_warning)

        else: #the object is of type job

            #get the rate limit key
            rate_limit_key = get_rate_limit_key(job.job.url, rate_limit)

            print(rate_limit_key)

            if rate_limit_key is None:

                raise NotImplementedError("Rate limit not implemented for the given url")
            
            else:

                #refresh the rate limit
                refresh_rate_limit(default, current, last_reset, rate_limit_key)

                #check if the rate limit has been reached
                check, exceed_status = is_rate_limit_reached(current, rate_limit_key)

                if check:

                    #rate limit has been reached so we put the job back into the job queue
                    job_queue.put(job)

                    handle_rate_limit_exceeded(client_id, pipe_pool, sent_rate_limit_warning, exceed_status, last_rate_limit_warning)


                else:

                    #process the rate limit
                    process(current, rate_limit_key)

                    #send the job
                    write_end = pipe_pool[client_id]

                    if isinstance(write_end, list):

                        write_end[job_count[client_id]%len(write_end)].send(job)

                    else:

                        write_end.send(job)

                    job_count[client_id] += 1


    #main loop
    while cycle(frequency):

        #cleanup the process pool
        garbage_counter, to_close = cleanup_process_pool(garbage_counter, garbage_collection_threshhold, to_close)

        #get a job from the job_queue
        try:

            job: Job = job_queue.get(block=False)

            #frequency, adjusting_frequency = adjust_frequency(1, buffer, buffer_length, frequency, min_frequency, adjusting_frequency)

        except Empty:

            #frequency, adjusting_frequency = adjust_frequency(0, buffer, buffer_length, frequency, min_frequency, adjusting_frequency)

            continue
            
        #allocate a process to the client if it is not already allocated (default is STREAM mode)
        client_id = job.client_id

        if client_id not in process_pool:

            allocate_process(1, client_id, process_pool, pipe_pool, job_count, sent_rate_limit_warning, last_rate_limit_warning)

        #handle the job
        handle_job(job, client_id, process_pool, pipe_pool, to_close, job_count, sent_rate_limit_warning, last_rate_limit_warning)

        print(last_rate_limit_warning, sent_rate_limit_warning)
            

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

def broadcaster(broadcast_pool, connection_pool) -> None:

    """
    A function to broadcast the results to the respective clients.
    """

    while True:

        try:

            data = broadcast_pool.get()

        except Empty:

            continue

        #check if object is of type Status
        if isinstance(data, Status):

            #check if the state is closed_connection
            if data.details.state == States.closed_connection:

                connection_pool.pop(data.client_id)

                print(connection_pool)

            elif data.details.state == States.renamed_connection:

                connection_pool[data.details.instruction[0]] = connection_pool[data.client_id]

                connection_pool.pop(data.client_id)

                print(connection_pool)

            elif data.details.state == States.rate_limit_exceeded:

                connection_pool.get(data.client_id).send(encode_object(data.details))

            else: #TODO: handle other statuses

                #send the status to the client
                connection_pool.get(data.client_id).send(encode_object(data.details))

        else:

            #send the result to the client (it must be of type Result)
            connection_pool.get(data.client_id).send(encode_object(data.result))

