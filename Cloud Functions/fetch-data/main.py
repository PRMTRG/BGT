import subprocess
from google.cloud import storage
import time

def fetch_data(event, context):
    subprocess.run(["kaggle", "datasets", "download", "-p", "/tmp", "--force", "rsrishav/youtube-trending-video-dataset"])
    storage_client = storage.Client()
    bucket = storage_client.bucket("fetched-data-1")
    filename = "youtube-trending-video-dataset_" + str(int(time.time())) + ".zip"
    blob = bucket.blob(filename)
    blob.upload_from_filename("/tmp/youtube-trending-video-dataset.zip")

