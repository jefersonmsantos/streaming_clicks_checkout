import os
from google.oauth2 import service_account
from google.cloud import pubsub_v1

#PROJECT_ID = os.environ['PROJECT_ID']
#CLICK_TOPIC = os.environ['CLICK_TOPIC']
#CHECKOUT_TOPIC = os.environ['CHECKOUT_TOPIC']

PROJECT_ID='edc-igti-325912'
REDIS_PASSWORD='Redis2019!'
CLICK_TOPIC='clicks'
CHECKOUT_TOPIC='checkout'

dir = os.path.dirname(__file__)
sa_file = os.path.join(dir, 'sa.json')
credentials = service_account.Credentials.from_service_account_file(sa_file)

publisher = pubsub_v1.PublisherClient(credentials=credentials)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

def create_topic(project_id, topic_name):
    topic = f'projects/{project_id}/topics/{topic_name}'
    publisher.create_topic(name=topic)

def create_subscription(project_id, topic_name):

    topic = f'projects/{project_id}/topics/{topic_name}'
    subscription_name = f'projects/{project_id}/subscriptions/{topic_name}'

    subscriber.create_subscription(name=subscription_name, topic=topic)

for topic in [CLICK_TOPIC,CHECKOUT_TOPIC]:
    create_topic(PROJECT_ID, topic)
    create_subscription(PROJECT_ID, topic)