from atproto import parse_subscribe_repos_message, CAR
import json

def process_message(message, resolver, output_file, verbose, post_count, lock):
    """
    Process a single message from the firehose
    
    Args:
        message: Message from the firehose
        resolver: IdResolver instance
        output_file: File to write posts to
        verbose: Whether to print verbose output
        post_count: Shared counter for posts processed
        lock: Lock for thread-safe operations
    """
    try:
        commit = parse_subscribe_repos_message(message)
        if not hasattr(commit, 'ops'):
            return

        for op in commit.ops:
            if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                process_post(commit, op, resolver, output_file, verbose, post_count, lock)

    except Exception as e:
        print(f"Error processing message: {e}")

def process_post(commit, op, resolver, output_file, verbose, post_count, lock):
    """
    Process a single post operation
    
    Args:
        commit: Commit data
        op: Operation data
        resolver: IdResolver instance
        output_file: File to write posts to
        verbose: Whether to print verbose output
        post_count: Shared counter for posts processed
        lock: Lock for thread-safe operations
    """
    try:
        author_handle = resolve_author_handle(commit.repo, resolver)
        car = CAR.from_bytes(commit.blocks)
        for record in car.blocks.values():
            if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                post_data = extract_post_data(record, commit.repo, op.path, author_handle)
                save_post_data(post_data, output_file, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing record: {e}")

def resolve_author_handle(repo, resolver):
    """
    Resolve the author handle from the DID
    
    Args:
        repo: Repository DID
        resolver: IdResolver instance
    
    Returns:
        String handle or DID
    """
    try:
        resolved_info = resolver.did.resolve(repo)
        return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
    except Exception as e:
        print(f"Could not resolve handle for {repo}: {e}")
        return repo  # Fallback to DID

def extract_post_data(record, repo, path, author_handle):
    """
    Extract post data from a record
    
    Args:
        record: Post record
        repo: Repository DID
        path: Post path
        author_handle: Author handle
    
    Returns:
        Dictionary of post data
    """
    has_images = check_for_images(record)
    reply_to = get_reply_to(record)
    return {
        'text': record.get('text', ''),
        'created_at': record.get('createdAt', ''),
        'author': author_handle,
        'uri': f'at://{repo}/{path}',
        'has_images': has_images,
        'reply_to': reply_to
    }

def check_for_images(record):
    """
    Check if the post has images
    
    Args:
        record: Post record
    
    Returns:
        Boolean indicating presence of images
    """
    embed = record.get('embed', {})
    return (
        embed.get('$type') == 'app.bsky.embed.images' or
        (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
    )

def get_reply_to(record):
    """
    Get the URI of the post being replied to
    
    Args:
        record: Post record
    
    Returns:
        URI string or None
    """
    reply_ref = record.get('reply', {})
    return reply_ref.get('parent', {}).get('uri')

def save_post_data(post_data, output_file, verbose, post_count, lock):
    """
    Save post data to the output file
    
    Args:
        post_data: Dictionary of post data
        output_file: File to write to
        verbose: Whether to print verbose output
        post_count: Shared counter for posts processed
        lock: Lock for thread-safe operations
    """
    with lock:
        with open(output_file, 'a') as f:
            json.dump(post_data, f)
            f.write('\n')
    with post_count.get_lock():
        post_count.value += 1
    if verbose:
        print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")