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

class RateLimitsNotDefined(Exception):

    """
    An exception to raise when the rate limits are not defined.
    """

    def __init__(self, url: str) -> None:

        self.message = f"Rate limits not defined for {url}"

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message

class RenameFailed(Exception):

    """
    An exception to raise when the rename signal is not successful.
    """

    def __init__(self) -> None:

        self.message = "Rename failed, name already exists."

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message

class UnexpectedResponse(Exception):

    """
    An exception to raise when the response is unexpected.
    """

    def __init__(self, obj: any, expected: any) -> None:

        self.message = f"Unexpected response received: {obj}, expected: {expected}"

        super().__init__(self.message)

    def __str__(self) -> str:

        return self.message
    
class StreamModeFailed(Exception):

    """
    An exception to raise when the stream mode is not successful.
    """

    def __init__(self) -> None:

        super().__init__("Stream mode failed.")

    def __str__(self) -> str:

        return "Stream mode failed."
    
class BatchModeFailed(Exception):

    """
    An exception to raise when the batch mode is not successful.
    """

    def __init__(self) -> None:

        super().__init__("Batch mode failed.")

    def __str__(self) -> str:

        return "Batch mode failed."
