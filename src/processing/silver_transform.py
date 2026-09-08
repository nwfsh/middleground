"""
silver_transform.py — Bronze -> Silver
Flattens raw comment JSON, filters obvious spam/bot noise,
scores sentiment via cardiffnlp/twitter-roberta-base-sentiment-latest,
writes cleaned + scored output as Parquet.
"""

import glob # finds the file on computer that matches a pattern 
import json
import re # import regular expressions -> used for filtering spam messages, mini pattern matching language 
from pathlib import Path 
# letting u write file paths easier like "bronze" + "/" + "raw" + "/" + "comments_abc.json"
# into Path("bronze") / "raw" / "comments_abc.json" with is less fragile


import pandas as pd # python library to be able to work with table of data ( usually for dsci but we r using for a small use case)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType

BRONZE_DIR = Path("bronze/raw")
SILVER_DIR = Path("silver")
SILVER_DIR.mkdir(exist_ok=True)


# flatten raw Bronze JSON into one row per comment 
def flatten_comment_files(spark) -> "pyspark.sql.DataFrame":
    """
    read every comment_* file in bronze, walks the nested commentThread
    strucutre, and produce one row per comment 

    """
    rows = []

    for filepath in glob.glob(str(BRONZE_DIR / "comments_*.json")):
        # video_id is embedded in the filename: comments_{video_id}_{timestamp}.json
        video_id = Path(filepath).stem.split("_")[1]

        with open(filepath) as f:
            data = json.load(f)

        # Your ingest_video() shape: {"threads": [...pages...], "full_replies": [...]}
        # check the json file to know how the shape looks like 
        pages = data.get("threads", data if isinstance(data, list) else [])

        for page in pages:
            for item in page.get("items", []):
                top = item["snippet"]["topLevelComment"]["snippet"] # go between the two snippet likes in a json file 
                rows.append({
                    "comment_id": item["id"],
                    "parent_comment_id": None, 
                    "video_id": video_id,
                    "text": top.get("textOriginal", ""),
                    "like_count": top.get("likeCount", 0),
                    "published_at": top.get("publishedAt"),
                    "is_reply": False,
                    "total_reply_count": item["snippet"].get("totalReplyCount", 0),
                })
                # Inline replies (up to 5, per API default)
                for reply in item.get("replies", {}).get("comments", []):
                    r = reply["snippet"]
                    rows.append({
                        "comment_id": reply["id"],
                        "parent_comment_id": item["id"], 
                        "video_id": video_id,
                        "text": r.get("textOriginal", ""),
                        "like_count": r.get("likeCount", 0),
                        "published_at": r.get("publishedAt"),
                        "is_reply": True,
                        "total_reply_count": 0,
                    })

        # Fully-expanded replies (beyond the inline 5, from your reply-completeness fix)
        for thread in data.get("full_replies", []):
            for page in thread.get("pages", []):
                for comment in page.get("items", []):
                    r = comment["snippet"]
                    rows.append({
                        "comment_id": comment["id"],
                        "parent_comment_id": item["id"], 
                        "video_id": video_id,
                        "text": r.get("textOriginal", ""),
                        "like_count": r.get("likeCount", 0),
                        "published_at": r.get("publishedAt"),
                        "is_reply": True,
                        "total_reply_count": 0,
                    })

    df = spark.createDataFrame(rows)
    # Dedup: same comment can appear across multiple ingestion runs/pages
    df = df.dropDuplicates(["comment_id"]) # if its comming from the same person + same text
    return df


