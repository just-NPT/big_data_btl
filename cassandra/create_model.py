from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()

# session.execute("CREATE KEYSPACE review\
#     WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}")

session.execute("USE review")

session.execute("DROP TABLE IF EXISTS review_data")

session.execute("CREATE TABLE review_data (\
   review_id text,\
   user_id text,\
   product_id text,\
   star_rating double,\
   review_headline text,\
   review_body text,\
   scores double,\
   datetime_occured timestamp,\
   PRIMARY KEY (review_id)\
)")

session.execute("DROP TABLE IF EXISTS checkout_data")

session.execute("CREATE TABLE checkout_data (\
   checkout_id text,\
   user_id double,\
   product_id double,\
   price double,\
   payment_method double,\
   num_product double,\
   shipping_address double,\
   billing_address double,\
   user_agent double,\
   ip_address double,\
   datetime_occured double,\
   PRIMARY KEY (checkout_id)\
)")

session.shutdown()
