"""
Saves post data to shared dictionary and list, ensuring thread-safety.
Updates the post count and optionally prints a verbose message with the post's author and text preview.
"""

def _process_post_json(post_data, posts_dict, posts_list, verbose, post_count, lock):
    """
    Save post data to the in-memory collections.
    
    Args:
        post_data: Dictionary containing post data
        posts_dict: A shared dictionary for storing posts indexed by URI
        posts_list: A shared list for storing posts in order of collection
        verbose: Boolean flag indicating whether to print verbose output
        post_count: Shared counter for tracking the number of posts processed
        lock: A multiprocessing Lock for thread-safe operations
    """
    uri = post_data['uri']
    with lock:
        # Store the post in both collections
        posts_dict[uri] = post_data
        posts_list.append(post_data)
    
    with post_count.get_lock():
        post_count.value += 1
    
    if verbose:
        print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")