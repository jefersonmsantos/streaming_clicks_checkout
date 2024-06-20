import os

import argparse
import json
import random
import redis
from datetime import datetime
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from uuid import uuid4

#import psycopg2
#from confluent_kafka import Producer
from faker import Faker

PROJECT_ID = os.environ['PROJECT_ID']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True, password = REDIS_PASSWORD)
fake = Faker()

dir = os.path.dirname(__file__)
sa_file = os.path.join(dir, 'sa.json')
credentials = service_account.Credentials.from_service_account_file(sa_file)

def push_to_redis(id, entity,object):
    redis_client.hset(f'{entity}_{str(id)}', mapping=object)

# Generate user data
def gen_user_data(num_user_records: int) -> None:
    for id in range(num_user_records):
        user = {"user_name":fake.user_name(), "password":fake.password()}
        push_to_redis(id,"user",user)

        product = {"name":fake.name(),"description":fake.text(),"price":fake.random_int(min=1, max=100)}
        push_to_redis(id,"product",product)
    return

# Generate a random user agent string
def random_user_agent():
    return fake.user_agent()

# Generate a random IP address
def random_ip():
    return fake.ipv4()


# Generate a click event with the current datetime_occured
def generate_click_event(user_id, product_id=None):
    click_id = str(uuid4())
    product_id = product_id or str(uuid4())
    product = fake.word()
    price = fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    url = fake.uri()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()

    click_event = {
        "click_id": click_id,
        "user_id": user_id,
        "product_id": product_id,
        "product": product,
        "price": price,
        "url": url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }

    return click_event


# Generate a checkout event with the current datetime_occured
def generate_checkout_event(user_id, product_id):
    payment_method = fake.credit_card_provider()
    total_amount = fake.pyfloat(left_digits=3, right_digits=2, positive=True)
    shipping_address = fake.address()
    billing_address = fake.address()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()

    checkout_event = {
        "checkout_id": str(uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "payment_method": payment_method,
        "total_amount": total_amount,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }

    return checkout_event

def push_to_pubsub(event , topic):
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_name = f'projects/{PROJECT_ID}/topics/{topic}'
    publisher.publish(topic_name, json.dumps(event).encode('utf-8'))

def gen_clickstream_data(num_click_records: int) -> None:
    for _ in range(num_click_records):
        user_id = random.randint(1, 100)
        click_event = generate_click_event(user_id)
        push_to_pubsub(click_event, 'clicks')

        # simulate multiple clicks & checkouts 50% of the time
        while random.randint(1, 100) >= 30:
            click_event = generate_click_event(
                user_id, click_event['product_id']
            )
            push_to_pubsub(click_event, 'clicks')

            push_to_pubsub(
                generate_checkout_event(
                    click_event["user_id"], click_event["product_id"]
                ),
                'checkouts',
            )
            print("---------CHECKOUT------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-nu",
        "--num_user_records",
        type=int,
        help="Number of user records to generate",
        default=100,
    )
    parser.add_argument(
        "-nc",
        "--num_click_records",
        type=int,
        help="Number of click records to generate",
        default=100000000,
    )
    args = parser.parse_args()
    gen_user_data(args.num_user_records)
    gen_clickstream_data(args.num_click_records)