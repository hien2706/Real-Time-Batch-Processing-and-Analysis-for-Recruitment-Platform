from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid
cluster = Cluster(['cassandra'])  # Use the service name from docker-compose
session = cluster.connect('de_project')  # keyspace 
def insert_into_cassandra(data):
    # Connect to Cassandra using the service name defined in docker-compose

    sql = """ INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) 
              VALUES ({},'{}','{}','{}','{}','{}','{}','{}')""".format(
        uuid.UUID(data['create_time']), data['bid'], data['campaign_id'], data['custom_track'], 
        data['group_id'], data['job_id'], data['publisher_id'], data['ts']
    )
    print(sql)
    session.execute(sql)
def consume():
    consumer = KafkaConsumer('TrackingTopic',
                             bootstrap_servers=['kafka:29092'],
                             auto_offset_reset='latest'
                             )

    for msg in consumer:
        print(f"Received message: {msg.value}")
        data = json.loads(msg.value)
        insert_into_cassandra(data)
if __name__ == '__main__':
    consume()