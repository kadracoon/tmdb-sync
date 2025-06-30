from pymongo import MongoClient
from collections import defaultdict
from rich import print
from rich.console import Console


# Настройки подключения
client = MongoClient("mongodb://localhost:27017/")
db = client["tmdb"]
reports = db["frame_reports"]
movies = db["movies_collection"]

# Группировка репортов
pipeline = [
    {"$group": {
        "_id": {"movie_id": "$movie_id", "frame_path": "$frame_path"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}}
]

aggregated = list(reports.aggregate(pipeline))

# movie_id → {frame_path → count}
movie_data = defaultdict(lambda: defaultdict(int))
for item in aggregated:
    movie_id = item["_id"]["movie_id"]
    frame_path = item["_id"]["frame_path"]
    count = item["count"]
    movie_data[movie_id][frame_path] = count

# Вывод
console = Console()
for movie_id, frame_counts in movie_data.items():
    movie = movies.find_one({"id": movie_id})
    title = movie.get("title_ru") or movie.get("title") or "Unknown Title"
    total = sum(frame_counts.values())

    console.print(f"\n[bold yellow]🎬 {title} (ID: {movie_id}) — {total} reports[/bold yellow]")
    for frame_path, count in sorted(frame_counts.items(), key=lambda x: x[1], reverse=True):
        console.print(f"    └─ {frame_path} — [red]{count}[/red] reports")
