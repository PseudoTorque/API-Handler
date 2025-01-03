"""
All the exceptions used in the project.
"""

class JobQueueEmpty(Exception):

    """
    An exception to raise when the job queue is empty.
    """

    def __init__(self, message: str) -> None:

        self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message

class BroadcastQueueEmpty(Exception):

    """
    An exception to raise when the broadcast queue is empty.
    """

    def __init__(self, message: str) -> None:

        self.message = message

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message

class RateLimitsNotDefined(Exception):

    """
    An exception to raise when the rate limits are not defined.
    """

    def __init__(self, url: str) -> None:

        self.message = f"Rate limits not defined for {url}"

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message
