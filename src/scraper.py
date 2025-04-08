import time
import multiprocessing
import sys
import signal
from atproto import DidInMemoryCache, IdResolver
from src.client import client_process, worker_process
from src.processor import process_message

class FirehoseScraper:
    def __init__(self, output_file="bluesky_posts.jsonl", verbose=False, num_workers=4):
        """
        Initialize the FirehoseScraper
        
        Args:
            output_file: Path to output file
            verbose: Whether to print verbose output
            num_workers: Number of worker processes
        """
        self.output_file = output_file
        self.post_count = multiprocessing.Value('i', 0)  # Shared integer
        self.start_time = None
        self.cache = DidInMemoryCache() 
        self.resolver = IdResolver(cache=self.cache)
        self.verbose = verbose
        self.queue = multiprocessing.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.stop_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()  # For thread-safe file writing
        self.client_proc = None
        
        # Create empty output file
        with open(self.output_file, 'w') as f:
            pass

    def start_collection(self, duration_seconds=None, post_limit=None):
        """
        Start collecting posts from the firehose
        
        Args:
            duration_seconds: Maximum duration in seconds, or None for no limit
            post_limit: Maximum number of posts to collect, or None for no limit
        """
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        # Start worker processes
        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(
                    self.queue, 
                    self.output_file, 
                    self.verbose, 
                    self.post_count, 
                    self.lock, 
                    self.stop_event,
                    process_message
                )
            )
            p.start()
            self.workers.append(p)

        # Handle KeyboardInterrupt in the main process
        def signal_handler(sig, frame):
            print("\nCollection stopped by user.")
            self._stop_collection()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        while True:
            # Start the client in a separate process
            self.client_proc = multiprocessing.Process(
                target=client_process,
                args=(self.queue, self.stop_event)
            )
            self.client_proc.start()

            # Monitor the collection
            try:
                while True:
                    if self.stop_event.is_set():
                        break
                    if duration_seconds and time.time() > end_time:
                        print("\nTime limit reached.")
                        self._stop_collection()
                        break
                    elif post_limit and self.post_count.value >= post_limit:
                        print("\nPost limit reached.")
                        self._stop_collection()
                        break
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

    def _stop_collection(self):
        """
        Stop the collection and print summary
        """
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.client_proc and self.client_proc.is_alive():
            self.client_proc.terminate()
            self.client_proc.join()

        # Wait for all worker processes to finish
        for p in self.workers:
            if p.is_alive():
                p.terminate()
            p.join()

        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.post_count.value / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count.value} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.output_file}")