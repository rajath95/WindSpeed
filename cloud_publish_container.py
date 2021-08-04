import requests
import os
import time
import json
import logging
from google.cloud import pubsub_v1

class Publish:
    def __init__(self):
        self.project_id = "elevated-summer-320003"
        self.topic_id = "Weather"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cloudcredentials.json"
        logging.basicConfig(level=logging.INFO)


    def fetch_weather(self):
        try:
            api_key = os.environ['weather_api_key']

        #set key as envt var
            url = 'http://api.weatherapi.com/v1/current.json?key='+api_key+'&q=Boston&aqi=no'
            response = requests.get(url)
            logging.info("successfully pinged weather api")

        except:
            logging.error("error during get request on url")
            return

        try:
            string = response.json()
            json_string = json.dumps(string)
            logging.info("converted json response data to string")
            logging.info(json_string)
        except:
            logging.error("error during json conversion")

        return json_string


    def publish_msg(self):

        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(self.project_id,self.topic_id)

        # fetch_weather should be byte string
            weather_data = self.fetch_weather()
            weather_data = weather_data.encode("utf-8")
            future = publisher.publish(topic_path,weather_data)
            logging.info("Successfully published message to topic")
            print(future.result())
        except:
            raise Exception("Failed to run publish")

while True:
    pub = Publish()
    pub.publish_msg()
    time.sleep(900)
#pub.fetch_weather()
