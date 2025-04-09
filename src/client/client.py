"""
Handles connecting to and receiving messages from the Bluesky firehose.
It provides functions for starting a client process that pushes firehose messages
to a shared queue for further processing.

functions:
client_process: Connects to the Bluesky firehose, forwarding messages to a shared queue until the stop event is triggered.
start_client_process: Starts a new process for the firehose client.
"""

from atproto import FirehoseSubscribeReposClient
import multiprocessing


def client_process(queue, stop_event):
    """
    Process that connects to the Bluesky firehose and forwards messages to a queue.
    
    Args:
        queue: A multiprocessing Queue where firehose messages will be placed
        stop_event: A multiprocessing Event that signals when to stop the client
    """
    client = FirehoseSubscribeReposClient()
    
    def message_handler(message):
        """
        Callback function that receives messages from the firehose.
        Places each message in the shared queue for processing.
        
        Args:
            message: A raw firehose message
        """
        if stop_event.is_set():
            client.stop()
            return
        queue.put(message)
    
    try:
        # Start the firehose client with our message handler
        client.start(message_handler)
    except Exception as e:
        if not stop_event.is_set():
            print(f"Client process error: {e}")


def start_client_process(queue, stop_event):
    """
    Start a new process for the firehose client.
    
    Args:
        queue: A multiprocessing Queue where firehose messages will be placed
        stop_event: A multiprocessing Event that signals when to stop the client
        
    Returns:
        The created client process object
    """
    client_proc = multiprocessing.Process(
        target=client_process,
        args=(queue, stop_event)
    )
    client_proc.start()
    return client_proc