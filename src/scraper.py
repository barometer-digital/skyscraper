"""
Main module for collecting posts from the Bluesky firehose and storing them in memory.
Provides a FirehoseScraper class and a command-line interface.
"""

from .client.client import start_client_process
from .process.workers import worker_process
from atproto import DidInMemoryCache, IdResolver
import multiprocessing
import time
import signal
import sys
import json
import argparse
from datetime import datetime


class SkyScraper:
    """
    A class for collecting posts from the Bluesky firehose and storing them in memory.
    
    Attributes:
        post_count: Shared counter for tracking the number of posts processed
        verbose: Boolean flag indicating whether to print verbose output
        num_workers: Number of worker processes to use
        queue: A multiprocessing Queue for passing messages between processes
        stop_event: A multiprocessing Event for signaling processes to stop
        lock: A multiprocessing Lock for thread-safe operations
        posts_dict: A shared dictionary for storing posts indexed by URI
        posts_list: A shared list for storing posts in order of collection
    """
    
    def __init__(self, verbose=False, num_workers=4):
        """
        Initialize the FirehoseScraper.
        
        Args:
            verbose: Boolean flag indicating whether to print verbose output
            num_workers: Number of worker processes to use
        """
        self.post_count = multiprocessing.Value('i', 0)  # Shared integer counter
        self.start_time = None
        self.cache = DidInMemoryCache() 
        self.resolver = IdResolver(cache=self.cache)
        self.verbose = verbose
        self.queue = multiprocessing.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.stop_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()  # For thread-safe data structures
        self.client_proc = None
        
        # In-memory storage for posts
        self.manager = multiprocessing.Manager()
        self.posts_dict = self.manager.dict()  # Indexed by URI for quick lookups
        self.posts_list = self.manager.list()  # Ordered as collected

    def start_collection(self, duration_seconds=None, post_limit=None):
        """
        Start collecting posts from the firehose.
        
        Args:
            duration_seconds: Optional duration in seconds to collect posts
            post_limit: Optional limit on the number of posts to collect
            
        Returns:
            A tuple of (posts_dict, posts_list) containing the collected posts
        """
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        # Start worker processes to process firehose messages
        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(
                    self.queue, 
                    self.posts_dict, 
                    self.posts_list,
                    self.verbose, 
                    self.post_count, 
                    self.lock, 
                    self.stop_event
                )
            )
            p.start()
            self.workers.append(p)

        # Handle keyboard interrupts in the main process
        def signal_handler(sig, frame):
            print("\nCollection stopped by user.")
            self._stop_collection()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Main collection loop - will retry if client disconnects
        while True:
            # Start the client in a separate process
            self.client_proc = start_client_process(self.queue, self.stop_event)

            # Monitor the collection and check for stopping conditions
            try:
                while True:
                    if self.stop_event.is_set():
                        break
                        
                    # Check time limit
                    if duration_seconds and time.time() > end_time:
                        print("\nTime limit reached.")
                        self._stop_collection()
                        break
                        
                    # Check post limit
                    elif post_limit and self.post_count.value >= post_limit:
                        print("\nPost limit reached.")
                        self._stop_collection()
                        break
                        
                    # Check if client is still running
                    if not self.client_proc.is_alive():
                        if not self.stop_event.is_set():
                            # Client process exited unexpectedly
                            print("\nClient process exited unexpectedly.")
                            self._stop_collection()
                            break
                        else:
                            # Stop event is set; exit the loop
                            break
                            
                    time.sleep(1)
                else:
                    # If the collection completed successfully, break out of the retry loop
                    break
                    
                if self.stop_event.is_set():
                    break
                    
            except KeyboardInterrupt:
                print("\nCollection interrupted by user.")
                self._stop_collection()
                break
                
            except Exception as e:
                error_details = f"{type(e).__name__}: {str(e)}" if str(e) else f"{type(e).__name__}"
                print(f"\nConnection error: {error_details}")

        self._stop_collection()
        return dict(self.posts_dict), list(self.posts_list)

    def _stop_collection(self):
        """
        Stop the collection and print a summary.
        """
        if not self.stop_event.is_set():
            self.stop_event.set()

        # Terminate client process if still running
        if self.client_proc and self.client_proc.is_alive():
            self.client_proc.terminate()
            self.client_proc.join()

        # Wait for all worker processes to finish
        for p in self.workers:
            if p.is_alive():
                p.terminate()
            p.join()

        # Print collection statistics
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.post_count.value / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count.value} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Posts stored in memory")
        
    def get_collected_posts(self):
        """
        Return the collected posts.
        
        Returns:
            A tuple of (posts_dict, posts_list) containing the collected posts
        """
        return dict(self.posts_dict), list(self.posts_list)

