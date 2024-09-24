from kafka import KafkaProducer
import json
import time
import cassandra
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import  Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
import random
from datetime import datetime, timedelta


def generating_dummy_data():
    publisher = [str(num) for num in range(1, 39)]
    job_list = [str(num) for num in range(1, 241)]
    campaign_list = ['1','4','5','7','10','11','33','48','54','90','91','92']
    group_list = ['10','11','12','13','14','15','16','17','18','19','34']
    bid_list = ['0', '1','2','3','4','5','6','7']
    
    create_time = str(cassandra.util.uuid_from_time(datetime.now()))  # Convert to string for JSON serialization
    bid = random.choice(bid_list)
    interact = ['click', 'conversion', 'qualified', 'unqualified']
    custom_track = random.choices(interact, weights=(70, 10, 10, 10))[0]
    job_id = random.choice(job_list)
    publisher_id = random.choice(publisher)
    group_id = random.choice(group_list)
    campaign_id = random.choice(campaign_list)
    
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Already in string format
    
    # Return JSON serializable data
    return {
        "create_time": create_time,
        "bid": bid,
        "custom_track": custom_track,
        "job_id": job_id,
        "publisher_id": publisher_id,
        "group_id": group_id,
        "campaign_id": campaign_id,
        "ts": ts
    }
        
        
def produce():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge the message
        retries=5,   # Retry a few times if the message fails to send
    )

    while True:
        
        message = generating_dummy_data()
        print(message)
        future = producer.send('TrackingTopic', message)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message sent successfully. Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")

        producer.flush()  # Wait for any outstanding messages to be delivered
        
        time.sleep(5)
        
    # producer.flush()  # Wait for any outstanding messages to be delivered
    # producer.close()  # Close the producer to avoid resource leaks
if __name__ == "__main__":
    produce()
