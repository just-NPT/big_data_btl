import glob
from faker import Faker
import pandas as pd

# import psycopg2
directory_path = './raw-data'
fake = Faker()
csv_files = glob.glob(f'{directory_path}/*.csv')
products_df = None
reviews_df = None
for file in csv_files:
    df = pd.read_csv(file,index_col=0)
    product_df = df[['product_title', 'product_category']]
    review_df = df[['product_category', 'star_rating', 'review_headline', 'review_body']]
    product_df['price'] = [fake.pyfloat(left_digits=2, right_digits=2, positive=True) for i in range(len(product_df))]
    products_df = pd.concat([products_df, product_df], ignore_index=True)
    reviews_df = pd.concat([reviews_df, review_df], ignore_index=True)
# print(products_df[['product_title','product_category']])
# print(reviews_df)
products_df['product_id'] = products_df.reset_index().index
products_df.to_csv('product.csv',index=False)
reviews_df.to_csv('review.csv',index=False)

