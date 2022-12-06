import base64
from google.cloud import bigquery
import time
import json


def import_to_bigquery(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    j = json.loads(pubsub_message)
    export_metadata_uri = f'gs://{j["bucket"]}/{j["name"]}'
    if not export_metadata_uri.endswith("_youtube_trending_data.export_metadata"):
        return
    bq_table_name = export_metadata_uri[-40:-38]

    project_id = "bgt1-312618"
    bg_dataset_id = "1"
    job_id = ("import-datastore-export-" + str(time.time())).replace(".","")

    client = bigquery.Client()
    dataset = bigquery.dataset.DatasetReference(project_id, bg_dataset_id)
    table = bigquery.table.TableReference(dataset, bq_table_name)
    config = bigquery.job.LoadJobConfig(source_format="DATASTORE_BACKUP", write_disposition="WRITE_TRUNCATE")
    job = bigquery.job.LoadJob(job_id, export_metadata_uri, table, client, job_config=config)
    job.result()


