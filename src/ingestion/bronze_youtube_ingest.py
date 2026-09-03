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

# helper function to get all the remaining replies in get_all_comments 
def get_remaining_replies(parent_id: str) -> dict:
    """Pull full reply list for a thread with more replies than were inlined."""
    replies = []
    page_token = None
    while True:
        params = {
            "part": "snippet",
            "parentId": parent_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(f"{BASE_URL}/comments", params=params)
        resp.raise_for_status()
        data = resp.json()
        replies.append(data)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return {"parentId": parent_id, "pages": replies}


# pls note this does not remove the duplicate of the top 4 replies, this is raw ingestion rn, will
# remove duplicate replies later
def get_all_comments(video_id: str) -> dict:
    """
    Get all comment threads + full replies for a video.
    - commentThreads.list: 1 unit per page (max 100 threads), returns up to 4 replies inline
    - comments.list: 1 unit per page for threads where replies were truncated
    Returns {"threads": [...raw pages...], "full_replies": [...per truncated thread...]}
    """
    comments = []
    threads_needing_full_replies = []
    page_token = None

    while True:
        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(f"{BASE_URL}/commentThreads", params=params)

        if resp.status_code == 403:
            print(f"403 error: {resp.json().get('error', {}).get('message')}")
            raise RuntimeError("Stopped early — pull is INCOMPLETE, do not treat as done")

        resp.raise_for_status()
        data = resp.json()
        comments.append(data)

        for item in data.get("items", []):
            total = item["snippet"]["totalReplyCount"]
            inlined = len(item.get("replies", {}).get("comments", []))
            if total > inlined:
                threads_needing_full_replies.append(item["id"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"{len(threads_needing_full_replies)} threads have uncaptured replies, fetching...")
    full_replies = [get_remaining_replies(pid) for pid in threads_needing_full_replies]

    return {"threads": comments, "full_replies": full_replies}


def land_raw_json(payload, video_id: str, kind: str):
    """Write raw JSON to disk, timestamped, untouched. This IS the bronze layer."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") # get current time in UTC and reformat the string 
    out_path = OUTPUT_DIR / f"{kind}_{video_id}_{ts}.json" # build the full file path 
    with open(out_path, "w") as f: # write python dicts u got earlier to the file as formatted JSON 
        json.dump(payload, f, indent=2)
    print(f"Landed {kind} → {out_path}") 

## joining all the helper functions together 
def ingest_video(video_id: str):
    print(f"Pulling metadata for {video_id}...")
    metadata = get_youtube_metadata(video_id)
    land_raw_json(metadata, video_id, "video_metadata")

    print(f"Pulling comments for {video_id}...")
    comments = get_all_comments(video_id)
    land_raw_json(comments, video_id, "comments")

    thread_pages = comments["threads"] ## get the dict 
    total_top_level = sum(len(p.get("items", [])) for p in thread_pages) # count how many comments in item list and add tgt 
    ## giving u a quick summary at the end 
    print(f"Done. {len(thread_pages)} thread pages, ~{total_top_level} top-level comments, {len(comments['full_replies'])} threads fully expanded.")


if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Set YOUTUBE_API_KEY as an environment variable first.")

    VIDEO_IDS = [
        "giZy9gEydzM",
        "VtuuvyLEmG4",
    ]
    for video_id in VIDEO_IDS:
        ingest_video(video_id)