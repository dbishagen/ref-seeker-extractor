import time

class Timer:
    """
    A simple timer class that allows to measure the duration of code execution in milliseconds.

    This class uses the time module to measure the start and end times of code execution and calculates the
    elapsed time. The __enter__ method is called when the with statement is entered and records the start time.
    The __exit__ method is called when the with statement is exited and calculates the elapsed time and prints
    it to the console.
    """

    # Source: ChatGPT
    def __enter__(self):
        # Record the starting time
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        # Record the ending time
        self.end = time.monotonic()

        # Calculate the execution time in seconds, milliseconds, and nanoseconds
        self.seconds = self.end - self.start

        # Print the execution time in all three units
        print(
            f"Execution time: {self.seconds:.6f} seconds")


"""
Usage:

with Timer():
    # your code here
"""