"""
Processes firehose messages from a queue, updating shared data structures with post information. 
It uses a stop_event to gracefully halt processing and ensures thread-safety with a lock.

"""
from atproto import parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
from .firehose_handlers import handle_firehose_message
import multiprocessing


def worker_process(queue, posts_dict, posts_list, verbose, post_count, lock, stop_event):
    """
    Worker process that processes messages from the firehose queue.
    
    Args:
        queue: A multiprocessing Queue containing firehose messages
        posts_dict: A shared dictionary for storing posts indexed by URI
        posts_list: A shared list for storing posts in order of collection
        verbose: Boolean flag indicating whether to print verbose output
        post_count: Shared counter for tracking the number of posts processed
        lock: A multiprocessing Lock for thread-safe operations
        stop_event: A multiprocessing Event that signals when to stop processing
    """
    resolver = IdResolver(cache=DidInMemoryCache())
    while not stop_event.is_set():
        try:
            # Get a message from the queue with a timeout to check stop_event periodically
            message = queue.get(timeout=1)
            handle_firehose_message(message, resolver, posts_dict, posts_list, verbose, post_count, lock)
        except multiprocessing.queues.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")


