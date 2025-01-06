"""
Interface for the client to send jobs and signals to the server.
"""

from socket import socket, AF_INET, SOCK_STREAM
from time import time
from requests import Response

from utils import encode_object, decode_object, DillProcess #Reformat object has not been imported as dill can serialize any object after passing.

from objects import Close, SwitchToBatch, SwitchToStream, Rename
from objects import APIJob
from objects import Details, States

from exceptions import RenameFailed, UnexpectedResponse, BatchModeFailed, StreamModeFailed

class ClientSocket:

    """
    A class to represent the socket.
    """

    def __init__(self, host: str, port: int) -> None:

        self.socket = socket(AF_INET, SOCK_STREAM)

        self.socket.connect((host, port))

    def send(self, obj: any) -> None:

        """
        Send an object to the server.

        Args:
            obj (any): The object to send to the server.
        """

        self.socket.send(encode_object(obj))

    def receive(self) -> any:

        """
        Receive an object from the server.

        Returns:
            any: The object received from the server.
        """

        return decode_object(self.socket)

    def ensure_non_blocking(self) -> None:

        """
        Ensure the socket is non-blocking.
        """

        if self.socket.getblocking():
            self.socket.setblocking(False)

    def ensure_blocking(self) -> None:

        """
        Ensure the socket is blocking.
        """

        if not self.socket.getblocking():
            self.socket.setblocking(True)

#currently we will not use this class for async communication as it is not needed.
class Client:

    """
    A class to represent the client.
    """

    def __init__(self, host: str, port: int, name: str) -> None:

        self.connection = ClientSocket(host, port)

        self.name = name

        self.rename(name)

    def rename(self, name: str) -> None:

        """
        Rename the client.

        Args:
            name (str): The name to rename the client to.
        """

        self.connection.send(Rename(name))

        self.connection.ensure_blocking()

        signal = self.connection.receive()

        if isinstance(signal, Details):

            if signal.state == States.rename_connection_failed:

                raise RenameFailed()
            
            elif signal.state == States.renamed_connection:

                return
        
        else:

            raise UnexpectedResponse(signal, Details)
    
    def close(self) -> None:

        """
        Close the client.
        """

        self.connection.send(Close())

        self.connection.ensure_blocking()

        signal = self.connection.receive()

        if isinstance(signal, Details):

            if signal.state == States.closed_connection:

                return

        else:

            raise UnexpectedResponse(signal, Details)
    
    def _switch_to_batch(self, number_of_workers: int) -> None:

        """
        Switch to batch mode.

        Args:
            number_of_workers (int): The number of workers to use.
        """

        self.connection.send(SwitchToBatch(number_of_workers))

        self.connection.ensure_blocking()

        signal = self.connection.receive()

        if isinstance(signal, Details):

            if signal.state == States.switched_to_batch:

                return
            
            elif signal.state == States.switch_to_batch_failed:

                raise BatchModeFailed()

        else:

            raise UnexpectedResponse(signal, Details)
        
    def _switch_to_stream(self) -> None:

        """
        Switch to stream mode.
        """

        self.connection.send(SwitchToStream())

        self.connection.ensure_blocking()

        signal = self.connection.receive()

        if isinstance(signal, Details):

            if signal.state == States.switched_to_stream:

                return
            
            elif signal.state == States.switch_to_stream_failed:

                raise StreamModeFailed()

        else:

            raise UnexpectedResponse(signal, Details)

    def process_batch_and_return_all_responses(self, batch: list[APIJob], number_of_workers: int) -> tuple[list[tuple[Response, float]], list[tuple[Details, float]]]:

        """
        Process a batch of jobs.

        Args:
            batch (list[APIJob]): The batch of jobs to process.
            number_of_workers (int): The number of workers to use.

        Returns:
            tuple[list[tuple[Response, float]], list[tuple[Details, float]]]: The responses and the signals received with their timestamps.
        """

        batch_size, buffer, signals_received = len(batch), [], []

        #send a signal to the server to switch to batch mode.
        self._switch_to_batch(number_of_workers)

        #populate ids in the API job objects to maintain order.
        for i, job in enumerate(batch):
            job.id = i

        #send the jobs to the server.
        for job in batch:
            self.connection.send(job)

        #receive the responses/signals from the server.
        while len(buffer) < batch_size:

            self.connection.ensure_non_blocking()

            result = self.connection.receive()

            if result is None or not result:
                continue

            if isinstance(result, Details):

                signals_received.append((result, time()))

            else:

                buffer.append((result[0], result[1], time()))

        #sort the buffer based on the id of the job.
        buffer.sort(key=lambda x: x[0])

        #only consider the response from the server.
        buffer = [(response, time) for _, response, time in buffer]

        #send a signal to the server to switch to stream mode.
        self._switch_to_stream()

        return buffer, signals_received

    def process_one_and_return_response(self, job: APIJob) -> tuple[Response, float]:

        """
        Process a single job.

        Args:
            job (APIJob): The job to process.

        Returns:
            tuple[Response, float]: The response from the server and its timestamp.
        """

        self.connection.send(job)

        self.connection.ensure_blocking()

        return (self.connection.receive()[1], time())
    
    def process_batch_and_yield_responses(self, batch: list[APIJob], number_of_workers: int) -> any:

        """
        Process a batch of jobs and yield the responses. An extra two yields are added to the generator 
        to send the entire buffer and the signals received along with their timestamps.

        Args:
            batch (list[APIJob]): The batch of jobs to process.
            number_of_workers (int): The number of workers to use.

        Returns:
            tuple[Response, float]: The response from the server and its timestamp.
        """

        batch_size, buffer, signals_received = len(batch), [], []

        #send a signal to the server to switch to batch mode.
        self._switch_to_batch(number_of_workers)

        #populate ids in the API job objects to maintain order.
        for i, job in enumerate(batch):
            job.id = i

        #send the jobs to the server.
        for job in batch:
            self.connection.send(job)

        #while the buffer is not full of responses of the expected count, keep receiving responses/signals from the server.
        internal = 0

        while len(buffer) < batch_size or internal < batch_size:

            self.connection.ensure_non_blocking()

            result = self.connection.receive()
            
            if len(buffer) < batch_size:

                if result is None or not result:
                    continue

                if isinstance(result, Details):

                    signals_received.append((result, time()))

                else:

                    buffer.append((result[0], result[1], time()))

            #find the response with the id of the job.
            for i in buffer:

                if i[0] == internal:

                    yield i[1]

                    internal += 1
            
        #send a signal to the server to switch to stream mode.
        self._switch_to_stream()

        #yield the entire buffer and the signals received along with their timestamps.
        yield buffer

        yield signals_received

