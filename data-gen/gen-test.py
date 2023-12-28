from datetime import datetime
import json
from uuid import uuid4
from faker import Faker
import pandas as pd
import random
from confluent_kafka import Producer
fake = Faker()

def push_to_kafka(event, topic):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    producer.produce(topic, json.dumps(event))
    producer.flush()
    print(f"Kafka event: {json.dumps(event)}")

def gen_product():
    product_df = pd.read_csv("./product.csv")
    # print(product_df)
    index = random.randrange(len(product_df)-1)
    product = product_df.iloc[index]
    # print(product)
    return {
        'product_id': product['product_id'],
        'product_title': product['product_title'],
        'product_category': product['product_category'],
        'price': product['price']
    }

def gen_review(category):
    review_df = pd.read_csv("./review.csv")
    review_df = review_df[review_df['product_category']==category]
    review = review_df.iloc[random.randint(0,len(review_df)-1)]
    return review

def generate_click_event(user_id, product=None):
    click_id = str(uuid4())
    product_id = product['product_id'] or str(uuid4())
    product_title = product['product_title'] or fake.word()
    price = product['price'] or fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    url = fake.uri()
    user_agent = fake.user_agent()
    ip_address = fake.ipv4()
    datetime_occured = datetime.now()

    click_event = {
        "click_id": click_id,
        "user_id": str(user_id),
        "product_id": str(product_id),
        "product_title": product_title,
        "price": price,
        "url": url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S"),
    }
    print(f"Create event: {click_event}")
    return click_event

def gen_clickstream_data(num_click_records: int) -> None:
    for _ in range(num_click_records):
        user_id = random.randint(1, 100)
        products = []
        for i in range(random.randint(1, 10)):
            product = gen_product()
            click_event = generate_click_event(user_id, product)
            push_to_kafka(click_event, 'clicks')
            products.append(product)

        # simulate multiple clicks & checkouts 50% of the time
        while random.randint(1, 100) >= 50:
            generate_checkout_event(user_id=user_id, products=products)


def generate_checkout_event(user_id, products):
    payment_method = fake.credit_card_provider()
    shipping_address = fake.address()
    billing_address = fake.address()
    user_agent = fake.user_agent()
    ip_address = fake.ipv4()
    datetime_occured = datetime.now()
    bill_id = str(uuid4())

    for product in products:
        checkout_event = {
            "checkout_id": str(uuid4()),
            "user_id": str(user_id),
            "product_id": str(product['product_id']),
            "price": product['price'],
            "payment_method": payment_method,
            "num_product": str(len(products)),
            "shipping_address": shipping_address,
            "billing_address": billing_address,
            "bill_id": bill_id,
            "user_agent": user_agent,
            "ip_address": ip_address,
            "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S")
        }
        push_to_kafka(checkout_event, 'checkout')
        while random.randint(1, 100) >= 50:
            review_event = generate_review_event(user_id=user_id, product=product)
            push_to_kafka(review_event, 'review')

def generate_review_event(user_id, product):
    print("Starting gen reviews")
    datetime_occured = datetime.now()
    review = gen_review(product['product_category'])
    review_event = {
        "review_id" : str(uuid4()),
        "user_id"   : str(user_id),
        "product_id": str(product['product_id']),
        "star_rating": str(review['star_rating']),
        "review_headline": review['review_headline'],
        "review_body" : review['review_body'],
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S")
    }
    return review_event

gen_clickstream_data(10)

