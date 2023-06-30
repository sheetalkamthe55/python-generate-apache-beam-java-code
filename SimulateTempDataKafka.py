from pykafka import KafkaClient
import threading
import sys
from datetime import datetime
import time
import json
from os.path import expanduser
import random

KAFKA_HOST = "localhost:9092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["echo-input"]

try: 
    with topic.get_sync_producer() as producer:  
      while True:
         # messages in json format
         # send message, topic: echo-input
         t = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
         outputValue = random.choice([20.0, 20.5, 21.0, 22.0, 22.5, 25.5, 30.0, 30.1, 31.5, 29.9, 35.0])
         msg_pub = {"component": "TempSensor", "id": "UT1233234", "temperature": "%f" % (outputValue) }
         encoded_message = json.dumps(msg_pub).encode("utf-8")
         producer.produce(encoded_message)
         time.sleep(30)
except:
      e = sys.exc_info()
      print ("end due to: ", str(e))