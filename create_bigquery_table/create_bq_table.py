import os
from google.oauth2 import service_account
from google.cloud import bigquery

PROJECT_ID = os.environ['PROJECT_ID']
BQ_DATASET = os.environ['BQ_DATASET']
BQ_TABLE = os.environ['BQ_TABLE']

dir = os.path.dirname(__file__)
sa_file = os.path.join(dir, 'sa.json')
credentials = service_account.Credentials.from_service_account_file(sa_file)

class Table(bigquery.Table):
    def set_primary_key(self, primary_key):
        if 'tableConstraints' not in self._properties:
            self._properties['tableConstraints'] = {}
        self._properties['tableConstraints']['primaryKey'] = primary_key   

client = bigquery.Client(project=PROJECT_ID,credentials=credentials)

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

schema = [
    bigquery.SchemaField("checkout_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("click_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("checkout_time", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("click_time", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_name", "STRING"),

]

table_ref = f'{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}'

primary_key={"columns": ["checkout_id"]}
table = Table(table_ref, schema=schema)
table.set_primary_key(primary_key)
table = client.create_table(table, exists_ok = True)