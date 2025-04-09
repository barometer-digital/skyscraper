"""
Processes a single firehose message, extracting and storing post data from "create" operations. 
It handles errors and skips non-post operations.
"""

from atproto import parse_subscribe_repos_message
from .post_processors import _extract_bluesky_post

def handle_firehose_message(message, resolver, posts_dict, posts_list, verbose, post_count, lock):
    """
    Process a single message from the firehose.
    
    Args:
        message: A raw firehose message
        resolver: An IdResolver for resolving DIDs to handles
        posts_dict: A shared dictionary for storing posts indexed by URI
        posts_list: A shared list for storing posts in order of collection
        verbose: Boolean flag indicating whether to print verbose output
        post_count: Shared counter for tracking the number of posts processed
        lock: A multiprocessing Lock for thread-safe operations
    """
    try:
        commit = parse_subscribe_repos_message(message)
        if not hasattr(commit, 'ops'):
            return

        # Process only create operations for posts
        for op in commit.ops:
            if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                _extract_bluesky_post(commit, op, resolver, posts_dict, posts_list, verbose, post_count, lock)

    except Exception as e:
        print(f"Error processing message: {e}")