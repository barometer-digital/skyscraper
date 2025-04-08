#!/usr/bin/env python3
import argparse
from datetime import datetime
from src.scraper import FirehoseScraper

def main():
    parser = argparse.ArgumentParser(description='Collect posts from the Bluesky firehose')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--time', type=int, help='Collection duration in seconds')
    group.add_argument('-n', '--number', type=int, help='Number of posts to collect')
    parser.add_argument('-o', '--output', type=str, 
                      default=f"bluesky_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
                      help='Output file path (default: bluesky_posts_TIMESTAMP.jsonl)')
    parser.add_argument('-v', '--verbose', action='store_true',
                      help='Print each post as it is collected')
    parser.add_argument('-w', '--workers', type=int, default=4,
                      help='Number of worker processes (default: 4)')

    args = parser.parse_args()

    scraper = FirehoseScraper(output_file=args.output, verbose=args.verbose, num_workers=args.workers)
    scraper.start_collection(duration_seconds=args.time, post_limit=args.number)

if __name__ == "__main__":
    main()