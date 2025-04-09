"""
This script processes Bluesky posts from commits, extracting key data like author handle, text, creation time, media, and reply information. 
It uses the atproto library to handle CAR data and ensures thread-safety during processing. 

functions:

_extract_bluesky_post: Main function for processing posts.
_format_post_metadata: Extracts post metadata.
_detect_post_media: Checks if the post includes media.
_extract_parent_post_uri: Gets the parent post URI for replies.

"""

from atproto import CAR
from .persistence import _process_post_json
from .resolver import _convert_did_to_handle


def _extract_bluesky_post(commit, op, resolver, posts_dict, posts_list, verbose, post_count, lock):
    """
    Process a single post operation from a commit.
    
    Args:
        commit: A commit object from the firehose
        op: An operation within the commit
        resolver: An IdResolver for resolving DIDs to handles
        posts_dict: A shared dictionary for storing posts indexed by URI
        posts_list: A shared list for storing posts in order of collection
        verbose: Boolean flag indicating whether to print verbose output
        post_count: Shared counter for tracking the number of posts processed
        lock: A multiprocessing Lock for thread-safe operations
    """
    try:
        # Resolve the author's handle from their DID
        author_handle = _convert_did_to_handle(commit.repo, resolver)
        
        # Extract the post record from the CAR blocks
        car = CAR.from_bytes(commit.blocks)
        for record in car.blocks.values():
            if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                post_data = _format_post_metadata(record, commit.repo, op.path, author_handle)
                _process_post_json(post_data, posts_dict, posts_list, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing record: {e}")


def _format_post_metadata(record, repo, path, author_handle):
    """
    Extract relevant post data from a record.
    
    Args:
        record: The post record
        repo: The DID of the repository (user)
        path: The path of the record
        author_handle: The handle of the author
        
    Returns:
        A dictionary containing the post data
    """
    has_images = _detect_post_media(record)
    reply_to = _extract_parent_post_uri(record)
    return {
        'text': record.get('text', ''),
        'created_at': record.get('createdAt', ''),
        'author': author_handle,
        'uri': f'at://{repo}/{path}',
        'has_images': has_images,
        'reply_to': reply_to
    }


def _detect_post_media(record):
    """
    Check if the post has images.
    
    Args:
        record: The post record
        
    Returns:
        Boolean indicating whether the post contains images
    """
    embed = record.get('embed', {})
    return (
        embed.get('$type') == 'app.bsky.embed.images' or
        (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
    )


def _extract_parent_post_uri(record):
    """
    Get the URI of the post being replied to.
    
    Args:
        record: The post record
        
    Returns:
        The URI of the parent post or None if not a reply
    """
    reply_ref = record.get('reply', {})
    return reply_ref.get('parent', {}).get('uri')