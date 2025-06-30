from collections import defaultdict
from pymongo import MongoClient


client = MongoClient("mongodb://mongo:27017")
db = client["tmdb"]
movies = db["movies_collection"]
reports = db["frame_reports"]


def update_frame_reports():
    # 1. Count reports by (movie_id, frame_url)
    counter = defaultdict(int)
    for report in reports.find():
        key = (report["movie_id"], report["frame_path"])
        counter[key] += 1

    print(f"Found {len(counter)} reported frame paths")

    # 2. Check movies and update reports in frames[]
    modified = 0
    for (movie_id, frame_path), count in counter.items():
        result = movies.update_one(
            {
                "id": movie_id,
                "frames.url": frame_path
            },
            {
                "$set": {
                    "frames.$.reports": count
                }
            }
        )
        if result.modified_count > 0:
            modified += 1

    print(f"Updated {modified} frames with report counts.")


if __name__ == "__main__":
    update_frame_reports()
