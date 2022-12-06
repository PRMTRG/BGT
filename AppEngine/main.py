# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_app]
# [START gae_python3_app]
from flask import Flask, current_app, request
from google.cloud import storage
from google.cloud import datastore
import io
import zipfile
import pandas
import time
import json
import base64
import os
import threading


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

app.config['PUBSUB_VERIFICATION_TOKEN'] = \
    os.environ['PUBSUB_VERIFICATION_TOKEN']
app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']

datastore_client = datastore.Client()
storage_client = storage.Client()
bucket_name = "fetched-data-1"
bucket = storage_client.bucket(bucket_name)


def get_data_from_csv_file_in_zip(zipbytes, filename):
    with zipfile.ZipFile(io.BytesIO(zipbytes)) as thezip:
        with thezip.open(filename) as thefile:
            df = pandas.read_csv(thefile)
            return df.to_dict("records")


def get_last_entry_video_id_and_trending_date(kind):
    query = datastore_client.query(kind=kind)
    query.order = ["-time_entity_added"]
    res = query.fetch(limit=1)
    entity = ''
    for x in res:
        entity = x
        break
    try:
        video_id = entity["video_id"]
        trending_date = entity["trending_date"]
        return video_id, trending_date
    except:
        return '2137', '2137'


def store_entry(data, kind):
    entity = datastore.Entity(key=datastore_client.key(kind))
    entity.update({
        "video_id" : data["video_id"],
        "title" : data["title"],
        "publishedAt" : data["publishedAt"],
        "channelId" : data["channelId"],
        "channelTitle" : data["channelTitle"],
        "categoryId" : data["categoryId"],
        "trending_date" : data["trending_date"],
        "tags" : data["tags"],
        "view_count" : data["view_count"],
        "likes" : data["likes"],
        "dislikes" : data["dislikes"],
        "comment_count" : data["comment_count"],
        "thumbnail_link" : data["thumbnail_link"],
        "comments_disabled" : data["comments_disabled"],
        "ratings_disabled" : data["ratings_disabled"],
        "description" : str(data["description"])[:100],
        "time_entity_added" : time.time()
    })
    datastore_client.put(entity)


def store_new_entries(key, data):
    print(key + " " + str(len(data)))
    name = key[:-4]
    last_id, last_trending_date = get_last_entry_video_id_and_trending_date(name)
    limit = -1
    mx = len(data) - 1
    #mx = 1000
    for i in range(mx, -1, -1):
        if data[i]["video_id"] == last_id and data[i]["trending_date"] == last_trending_date:
            limit = i
            break
    for i in range(limit+1, mx):
        if i % 10 == 0:
            print(i)
        store_entry(data[i], name)


def update_database_with_new_data(filename):
    csv_filenames = [
        "BR_youtube_trending_data.csv",
        "CA_youtube_trending_data.csv",
        "DE_youtube_trending_data.csv",
        "FR_youtube_trending_data.csv",
        "GB_youtube_trending_data.csv",
        "IN_youtube_trending_data.csv",
        "JP_youtube_trending_data.csv",
        "KR_youtube_trending_data.csv",
        "MX_youtube_trending_data.csv",
        "RU_youtube_trending_data.csv",
        "US_youtube_trending_data.csv"
    ]
    blob = bucket.blob(filename)
    zipbytes = blob.download_as_bytes()
    for csv_filename in csv_filenames:
        data = get_data_from_csv_file_in_zip(zipbytes, csv_filename)
        store_new_entries(csv_filename, data)


@app.route('/')
def hello():
    return "hello"


@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    if (request.args.get('token', '') !=
            current_app.config['PUBSUB_VERIFICATION_TOKEN']):
        return 'Invalid request', 400

    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])
    filename = json.loads(payload)["name"]
    if not filename.endswith(".zip"):
        return 'OK', 200
    update_database_with_new_data(filename)

    return 'OK', 200


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_app]
# [END gae_python38_app]
