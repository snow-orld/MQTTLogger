'''
file: mqtt_logger.py

log specified mqtt messages

author: Xueman Mou
date: 2022/07/23
version: 0.0.1
modified: 2022/08/02 16:05:00 GMT +0800

reference: https://www.emqx.com/en/blog/how-to-use-mqtt-in-python
developing environment: python 3.8.9
dependencies: os, sys, traceback, json, datetime, paha.mqtt, random
'''

import os, sys, traceback
import json
from datetime import datetime
from paho.mqtt import client as mqtt_client
import random

file_name = ""
topics = {}

def load_topic_file(file = 'topics.json'):
	# print('loading topic file %s ...' % file)

	fp = open(file, 'r')
	obj = json.load(fp)
	fp.close()

	topics = []
	for topic_info in obj['topics']:
		topics.append(topic_info['topicName'])
	
	return topics

def print_topics(topic_array):
	for topic in topic_array:
		print(topic)

def get_current_timestamp():
	now = datetime.now()
	return now

def create_new_log_file():
	global file_name
	file_name = 'mqtt_%s.log' % (get_current_timestamp().strftime('%Y%m%d%H%M%S'))

def start_logging():
	
	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			print('正在录制数据……')
			print('按组合键 CTRL + C 结束录制！')
		else:
			print('Failed to connect, return code %d\n', rc)

	def subscribe(client):
		topics = load_topic_file()
		for topic in topics:
			client.subscribe(topic)
		client.on_message = on_message

	def on_message(client, userdata, msg):
		topic = msg.topic
		m_decode = str(msg.payload.decode("utf-8","ignore"))
		message_handler(client, topic, m_decode)
		# print("message received: %s %s" % (topic, m_decode))

	def message_handler(client, topic, msg):
		update = False
		if topic not in topics:
			update = True
		elif topics[topic] != msg:
			update = True
		if update:
			topics[topic] = msg
			fp.write('%s %s %s\n' % (get_current_timestamp(), topic, msg))

	fp = open(file_name, 'a+', encoding='utf-8')
	broker = "127.0.0.1"
	port = 1883
	client_id = f'python-mqtt-{random.randint(0, 1000)}'

	# Set Connecting Client ID
	client = mqtt_client.Client(client_id)
	client.on_connect = on_connect
	client.connect(broker, port)
	
	subscribe(client)

	client.loop_forever()

def main():
	try:
		create_new_log_file()
		start_logging()
	except KeyboardInterrupt:
		print('录制成功：%s' % os.path.abspath(file_name))
	except Exception:
		traceback.print_exc(file=sys.stdout)
	sys.exit(0)
			
if __name__ == '__main__':
	main()