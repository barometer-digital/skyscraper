import os
from atproto import Client

def authenticate(self, username=None, password=None):
        """
        Authenticate with Bluesky.
        """
        # Try to use environment variables if not provided
        username = username or os.environ.get('BLUESKY_USERNAME')
        password = password or os.environ.get('BLUESKY_PASSWORD')
        
        if not username or not password:
            raise ValueError("Bluesky credentials required. Provide them as arguments or set BLUESKY_USERNAME and BLUESKY_PASSWORD environment variables.")
        
        try:
            self.client = Client()
            self.client.login(username, password)
            print(f"Authenticated as {username}")
            return True
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False