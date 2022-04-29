#!./venv/bin/python3
"""
Listing all topics (defaults to only with messages).
"""

import os
import sys
import configparser
import kafka

if not os.path.isfile('config.ini'):
    print("ERROR: Remember to copy config.sample.ini to your own config.ini file!")
    import sys

    sys.exit(1)

config = configparser.ConfigParser()
config.read('config.ini')
kafka_settings = config['kafka']
avro_settings = config['avro']

consumer = kafka.consumer.KafkaConsumer(kafka_settings, avro_settings, 1)

with_messages_only = len(sys.argv) == 1
consumer.list_topics(with_messages_only=with_messages_only)
