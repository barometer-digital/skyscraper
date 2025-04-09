"""
Resolves the author handle from a DID. Uses the resolver to fetch the handle and returns the first known handle. 
If resolution fails, it returns the original DID.

"""

def _convert_did_to_handle(repo, resolver):
    """
    Resolve the author handle from the DID.
    
    Args:
        repo: The DID of the repository (user)
        resolver: An IdResolver for resolving DIDs to handles
        
    Returns:
        The handle of the user or the original DID if resolution fails
    """
    try:
        resolved_info = resolver.did.resolve(repo)
        return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
    except Exception as e:
        print(f"Could not resolve handle for {repo}: {e}")
        return repo  # Fallback to DID