from google.cloud import bigquery
import logging
from io import StringIO
import pandas
from google.cloud.storage import Client


class Load_to_BQ:
    def __init__(self,event,context):
        self.event = event
        self.context = context
        self.bucket_name='weather-storage-bin'
        self.dataset_id = 'windspeed'
        self.table_id = "table1"
        logging.basicConfig(level=logging.INFO)


    def transform(self,event):
        try:
            file_name = event['name']
            storage_client = Client()
            bucket = storage_client.bucket(self.bucket_name)
            blob = bucket.get_blob(file_name)
            text = blob.download_as_text()
            df = pandas.read_csv(StringIO(text))
            transformed_df=df[["location.lat","location.lon","current.wind_kph","current.temp_c","current.last_updated"]]
            transformed_df.columns= ["latitude","longitude","windspeed","temperature","last_updated"]
            logging.info(transformed_df)
            logging.info("Transformed df to include relevant columns")
        except:
            logging.error("Failed to transform text")
        return transformed_df




    def load(self,dataframe):
        try:
            tablename = self.dataset_id+"."+self.table_id
            dataframe.to_gbq(tablename,if_exists="append")
            logging.info("Successfully loaded transformed data to BigQuery Table")
        except:
            logging.error("Failed to load data")
        return





def hello_gcs(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    #print('Event ID: {}'.format(context.event_id))
    #print('Event type: {}'.format(context.event_type))
    #print('Bucket: {}'.format(event['bucket']))
    #print('File: {}'.format(event['name']))
    #print('Metageneration: {}'.format(event['metageneration']))
    #print('Created: {}'.format(event['timeCreated']))
    #print('Updated: {}'.format(event['updated']))

    client = Load_to_BQ(event,context)
    dataframe = client.transform(event)
    client.load(dataframe)
    return
