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

        return self.group_priority > other.group_priority or (self.group_priority == other.group_priority and self.internal_priority > other.internal_priority)

class APIMethods:

    """
    A class that holds the constants for the API methods.
    """

    get = "GET"
    post = "POST"
    delete = "DELETE"
    update = "UPDATE"
    put = "PUT"

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

        return self.priority < other.priority

class Close(Signal):

    """
    A class to represent the close signal.
    """

    def __init__(self, graceful: bool = True) -> None:

        super().__init__("CLOSE", Priority(9, 9))

        self.graceful = graceful

class SwitchToBatch(Signal):

    """
    A class to represent the switch to batch mode signal.
    """

    def __init__(self, num_processes: int) -> None:

        super().__init__("SWITCH_TO_BATCH", Priority(0, 0))

        self.num_processes = num_processes

class SwitchToStream(Signal):

    """
    A class to represent the switch to stream mode signal.
    """

    def __init__(self) -> None:

        super().__init__("SWITCH_TO_STREAM", Priority(0, 0))

class Rename(Signal):

    """
    A class to represent the rename signal.
    """

    def __init__(self, new_name: str) -> None:

        super().__init__("RENAME", Priority(9, 9))

        self.new_name = new_name

class InternalInterrupt(Signal):

    """
    A class to represent the internal interrupt signal.
    """

    def __init__(self) -> None:
        super().__init__("INTERNAL_INTERRUPT", Priority(0, 0))

class RateLimitExceeded(Signal):

    """
    A class to represent the rate limit exceeded signal.
    """

    def __init__(self, exceed_status: list[bool]) -> None:
        super().__init__("RATE_LIMIT_EXCEEDED", Priority(0, 0))

        self.second = exceed_status[0]

        self.minute = exceed_status[1]

        self.hour = exceed_status[2]

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
    
    def __lt__(self, other) -> bool:

        """
        Check if the job is less than another job/signal based on the priority, required for PriorityQueue which returns min element.
        """

        return self.priority < other.priority

class States:

    """
    A class to hold the constants for the states for the details of a status.
    """

    closed_connection = "CLOSED_CONNECTION"

    renamed_connection = "RENAMED_CONNECTION"

    rename_connection_failed = "RENAME_CONNECTION_FAILED"

    switched_to_batch = "SWITCHED_TO_BATCH"

    switched_to_stream = "SWITCHED_TO_STREAM"

    switch_to_batch_failed = "SWITCH_TO_BATCH_FAILED"

    switch_to_stream_failed = "SWITCH_TO_STREAM_FAILED"

    rate_limit_exceeded = "RATE_LIMIT_EXCEEDED"

class Reasons:

    """
    A class to hold the constants for the reasons for the details of a status.
    """

    client_sent_close_packet = "CLIENT_SENT_CLOSE_PACKET"

    client_sent_rename_packet = "CLIENT_SENT_RENAME_PACKET"

    name_already_exists = "NAME_ALREADY_EXISTS"

    client_sent_switch_to_batch_packet = "CLIENT_SENT_SWITCH_TO_BATCH_PACKET"

    client_sent_switch_to_stream_packet = "CLIENT_SENT_SWITCH_TO_STREAM_PACKET"

    second_rate_limit_exceeded = "SECOND_RATE_LIMIT_EXCEEDED"

    minute_rate_limit_exceeded = "MINUTE_RATE_LIMIT_EXCEEDED"

    hour_rate_limit_exceeded = "HOUR_RATE_LIMIT_EXCEEDED"

class Instructions:

    """
    A class to hold the constants for the instructions for the details of a status.
    """

    #TODO: Add the constants for the instructions.

class Details:

    """
    A class to represent the details for a status.
    """

    def __init__(self, state: str, reason: str, instruction: list[any]) -> None:

        self.state = state

        self.reason = reason

        self.instruction = instruction
#dummy testing class
class Dummy:
    """
    A class to represent the API call (the job object). Processes will send objects of this class to be processed by the APIHandler.
    """
    url = "upstox"
    def __init__(self, payload: str, priority: Priority, id: int = None) -> None:

        """
        Initialize the API job.
        """
        self.id = id
        self.payload = payload
        self.priority = priority

    def call(self) -> str:

        """
        Make the API call through the requests library and return the response.
        """
        time.sleep(0.1)
        return (self.id, "gp: " + str(self.priority.group_priority) + " ip: " + str(self.priority.internal_priority))
    
    def __lt__(self, other) -> bool:

        """
        Check if the job is less than another job/signal based on the priority, required for PriorityQueue which returns min element.
        """

        return self.priority < other.priority
