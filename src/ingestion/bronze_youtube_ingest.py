# Pull one video's metadata + comments, land as raw JSON.
# Bronze layer: untouched, append-only, exactly as returned by the API.

import json
import os
from datetime import datetime, timezone
from pathlib import Path 

from dotenv import load_dotenv

import requests

load_dotenv()
API_KEY = os.environ.get("YOUTUBE_API_KEY")  
BASE_URL = "https://www.googleapis.com/youtube/v3"
OUTPUT_DIR = Path("bronze/raw")

def get_youtube_metadata(video_id: str) -> dict:
    """Pull raw video metadata (snippet, statistics, contentDetails)."""
    resp = requests.get(
        f"{BASE_URL}/videos", #build the full api 
        params={
            "part": "snippet,statistics,contentDetails, status",
            "id": video_id,
            "key": API_KEY,
        },
    )
    resp.raise_for_status()
    return resp.json()

## snippet : itle, description, publish date, channel ID, tags, category, thumbnails
## statistic: view count, like count, comment count
## contentDetails : duration, resolution, region restrictions, caption availability, we need it for dim_video 
## status : privacy status, upload status, whether it's embeddable, made-for-kids flag : to track if it has been taken down through tracking 

def get_all_comments(video_id: str) -> list[dict]:
    """
    get all full comment thread of a video, paginate all pagres
    top level comment + replies 
    note : one page of comments ( containing max 100 ) -> cost 1 unit
    youtube DATA API : max 10,000 units per day 
    """
    comments = []
    page_token = None

    while True:
        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": 100,  # API max per page
            "textFormat": "plainText",
            "key": API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token ## first time will be false 

        resp = requests.get(f"{BASE_URL}/commentThreads", params=params)

        if resp.status_code == 403:
            # Common cause: comments disabled on this video, or quota exceeded
            print(f"403 error: {resp.json().get('error', {}).get('message')}")
            break

        resp.raise_for_status() ## see if API call succeeded 
        data = resp.json()
        comments.append(data)  # land each raw page as-is, don't flatten yet

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return comments


def land_raw_json(payload, video_id: str, kind: str):
    """Write raw JSON to disk, timestamped, untouched. This IS the bronze layer."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") # get current time in UTC and reformat the string 
    out_path = OUTPUT_DIR / f"{kind}_{video_id}_{ts}.json" # build the full file path 
    with open(out_path, "w") as f: # write python dicts u got earlier to the file as formatted JSON 
        json.dump(payload, f, indent=2)
    print(f"Landed {kind} → {out_path}") 


def ingest_video(video_id: str):
    print(f"Pulling metadata for {video_id}...")
    metadata = get_youtube_metadata(video_id)
    land_raw_json(metadata, video_id, "video_metadata")

    print(f"Pulling comment thread for {video_id}...")
    comment_pages = get_all_comments(video_id)
    land_raw_json(comment_pages, video_id, "comments")

    total_top_level = sum(len(p.get("items", [])) for p in comment_pages)
    print(f"Done. {len(comment_pages)} pages, ~{total_top_level} top-level comments.")

## API response → Python dict (in memory) → JSON file (on disk)

if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Set YOUTUBE_API_KEY as an environment variable first.")

    VIDEO_ID = [
        "giZy9gEydzM",
        "VtuuvyLEmG4"
        # "Gw-vKxhxGIY" did earlier 
    ]
     # the 11-char id from the YouTube URL
    ingest_video(VIDEO_ID)