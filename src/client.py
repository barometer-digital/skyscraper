from atproto import FirehoseSubscribeReposClient
import multiprocessing

def client_process(queue, stop_event):
    """
    Process to connect to the firehose
    
    Args:
        queue: Multiprocessing queue to put messages
        stop_event: Event to signal stopping
    """
    client = FirehoseSubscribeReposClient()
    
    def message_handler(message):
        if stop_event.is_set():
            client.stop()
            return
        queue.put(message)
    
    try:
        client.start(message_handler)
    except Exception as e:
        if not stop_event.is_set():
            print(f"Client process error: {e}")

def worker_process(queue, output_file, verbose, post_count, lock, stop_event, process_message):
    """
    Worker process to handle messages from the queue
    
    Args:
        queue: Multiprocessing queue to get messages from
        output_file: File to write posts to
        verbose: Whether to print verbose output
        post_count: Shared counter for posts processed
        lock: Lock for thread-safe operations
        stop_event: Event to signal stopping
        process_message: Function to process messages
    """
    from atproto import IdResolver, DidInMemoryCache
    resolver = IdResolver(cache=DidInMemoryCache())
    
    while not stop_event.is_set():
        try:
            message = queue.get(timeout=1)
            process_message(message, resolver, output_file, verbose, post_count, lock)
        except multiprocessing.queues.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")