"""
All the constants used in the project.
"""

from functools import total_ordering
import time
from requests import request, Response

class PriorityPreset:

        """
        A class to represent constants for the priority.
        """

        critical = 0

        highest = 1

        high = 3

        medium = 5

        low = 7

        lowest = 9

#Priority object based on the assumption that the group priority is more important than the internal priority and a lower number is has more priority. priority ranges from 0 to 99.
@total_ordering
class Priority:

    """
    A class to represent the priority of a task.
    """

    

    def __init__(self, group_priority: int = 9, internal_priority: int = 9) -> None:

        """
        Initialize the priority (default is lowest priority).

        Arguments:
            group_priority (int): The priority of the group of the task.
            internal_priority (int): The internal priority of the task within the group.
        """

        self.group_priority = group_priority

        self.internal_priority = internal_priority


    def __str__(self) -> str:

        """
        Return the string representation of the priority.
        """

        return f"Group Priority: {self.group_priority}, Internal Priority: {self.internal_priority}"

    def __eq__(self, other: 'Priority') -> bool:

        """
        Check if the priority is equal to another priority.
        """

        return self.group_priority == other.group_priority and self.internal_priority == other.internal_priority
    
    def __lt__(self, other: 'Priority') -> bool:

        """
        Check if the priority is less than another priority.
        """

        return self.group_priority < other.group_priority or (self.group_priority == other.group_priority and self.internal_priority < other.internal_priority)

class APIMethods:

    """
    A class that holds the constants for the API methods.
    """

    get = "GET"
    post = "POST"
    delete = "DELETE"
    update = "UPDATE"
    put = "PUT"


class APIHeaders:

    """
    A class that holds the constants for the (Upstox specific) API headers.
    """

    auth = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    accept = {
        'Accept': 'application/json'
    }

    def basic(self, auth_token: str) -> dict:

        """
        Return the basic headers for the Upstox API.
        """

        return {
        'Authorization': "Bearer " + auth_token,
        'Accept': 'application/json'
        }

    def basic_for_post(self, auth_token: str) -> dict:

        """
        Return the headers for the Upstox API for POST requests.
        """

        return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': "Bearer " + auth_token
        }
    
class APIJob:
    """
    A class to represent the API call (the job object). Processes will send objects of this class to be processed by the APIHandler.
    """

    def __init__(self) -> None:

        """
        Initialize the API job.
        """
        self.id = None
        
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

        return (self.id, request(method=self.method, url=self.url, timeout=self.timeout, headers=self.headers, data=self.payload))
    
class Signals:

    """
    A class that holds the constants for the signals.
    """

    close = "CLOSE"

class SignalPriorities:

    """
    A class that holds the constants for the signal priorities.
    """

    close = Priority(9, 9)

class Signal:

    """
    A class to serve as a signal packet to the API Handler.
    """
    
    def __init__(self, signal: str, priority: Priority) -> None:
        
        self.signal = signal

        self.priority = priority

    def __lt__(self, other) -> bool:

        """
        Check if the signal is less than another signal/job based on the priority, required for PriorityQueue which returns min element.
        """

        if isinstance(other, Job):

            return self.priority < other.job.priority
        
        elif isinstance(other, Signal):

            return self.priority < other.priority

    
class Job:

    """
    A class to represent the job object.
    """

    def __init__(self, job: any, client_id: int) -> None:

        self.job = job

        self.client_id = client_id

    def __lt__(self, other) -> bool:

        """
        Check if the job is less than another job/signal based on the priority, required for PriorityQueue which returns min element.
        """

        if isinstance(other, Job):

            return self.job.priority < other.job.priority
        
        elif isinstance(other, Signal):

            return self.job.priority < other.priority

class Result:

    """
    A class to represent the result of a job.
    """

    def __init__(self, result: any, client_id: int) -> None:

        self.result = result

        self.client_id = client_id

class States:

    """
    A class to hold the constants for the states for the details of a status.
    """

    closed_connection = "CLOSED_CONNECTION"

class Reasons:

    """
    A class to hold the constants for the reasons for the details of a status.
    """

    client_sent_close_packet = "CLIENT_SENT_CLOSE_PACKET"

class Instructions:

    """
    A class to hold the constants for the instructions for the details of a status.
    """

    #TODO: Add the constants for the instructions.

class State:

    """
    A class to represent the state for the details of a status.
    """

    def __init__(self, state: str) -> None:

        self.state = state

class Reason:

    """
    A class to represent the reason for the details of a status.
    """

    def __init__(self, reason: str) -> None:

        self.reason = reason

class Instruction:

    """
    A class to represent the instructions for the details of a status
    """

    def __init__(self, instructions: list[str]) -> None:
        
        self.instructions = instructions

class Details:

    """
    A class to represent the details for a status.
    """

    def __init__(self, state: State, reason: Reason, instruction: Instruction) -> None:

        self.state = state

        self.reason = reason

        self.instruction = instruction

class Status:
    
    """
    A class to represent a status.
    """

    def __init__(self, details: Details, client_id: int) -> None:
        
        self.details = details

        self.client_id = client_id

#dummy testing class
class Dummy:
    """
    A class to represent the API call (the job object). Processes will send objects of this class to be processed by the APIHandler.
    """

    def __init__(self, payload: str, priority: Priority) -> None:

        """
        Initialize the API job.
        """

        self.payload = payload
        self.priority = priority

    def call(self) -> str:

        """
        Make the API call through the requests library and return the response.
        """
        time.sleep(2)
        return "gp: " + str(self.priority.group_priority) + " ip: " + str(self.priority.internal_priority)
