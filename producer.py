from kafka import KafkaProducer
import json
from data import get_registered_user
import time

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def get_partition(key,all,available):
	return 0

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
	value_serializer=json_serializer,
	partitioner = get_partition)

# (bootstrap_servers='192.168.33.10:9092', auto_offset_reset='earliest')


if __name__== "__main__":
	while 1==1:
		registered_user =get_registered_user()
		print(registered_user)
		producer.send("reg_usser",registered_user)
		time.sleep(4)