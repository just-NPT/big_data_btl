from datetime import datetime
import random
import pandas as pd
from confluent_kafka import Producer
random.seed(10)
review_df = pd.read_csv("./review.csv", index_col=False)
review_df = review_df[review_df['product_category']=='Baby']
review = review_df.iloc[1]
print(type(review['star_rating']))
print(type(review['review_headline']))
print(type(review['review_body']))
# print((review['star_rating'][0]))
# print(type(review['star_rating'][0]))
# print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# review_df = pd.read_csv("./review.csv",index_col=0)
# review_df = review_df[review_df['product_category']=='Baby']
# review = review_df.sample()
# print( {
#     'star_rating': review['star_rating'],
#     'review_headline': review['review_headline'][0],
#     'review_body': review['review_body'][0]
# })