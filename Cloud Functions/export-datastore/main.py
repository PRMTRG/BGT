import base64
import os

from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


# The default cache (file_cache) is unavailable when using oauth2client >= 4.0.0 or google-auth,
# and it will log worrisome messages unless given another interface to use.
datastore = build("datastore", "v1", cache=MemoryCache())
project_id = "bgt1-312618"


def datastore_export(event, context):

    bucket = "gs://datastore-exports-1"
    entity_filter = {"kinds": [
      "BR_youtube_trending_data",
      "CA_youtube_trending_data",
      "DE_youtube_trending_data",
      "FR_youtube_trending_data",
      "GB_youtube_trending_data",
      "IN_youtube_trending_data",
      "JP_youtube_trending_data",
      "KR_youtube_trending_data",
      "MX_youtube_trending_data",
      "RU_youtube_trending_data",
      "US_youtube_trending_data"
    ]}

    #if "kinds" in json_data:
    #    entity_filter["kinds"] = json_data["kinds"]

    request_body = {"outputUrlPrefix": bucket, "entityFilter": entity_filter}

    export_request = datastore.projects().export(
        projectId=project_id, body=request_body
    )
    response = export_request.execute()
    print(response)
