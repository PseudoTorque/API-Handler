"""
All the constants used in the project.
"""

from functools import total_ordering

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


    
