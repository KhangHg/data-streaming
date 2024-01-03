import logging

# from cassandra.cluster import Cluster
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streams
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)

#     print("Keyspace created successfully!")


# def create_table(session):
#     session.execute("""
#     CREATE TABLE IF NOT EXISTS spark_streams.created_users (
#         id UUID PRIMARY KEY,
#         first_name TEXT,
#         last_name TEXT,
#         gender TEXT,
#         address TEXT,
#         post_code TEXT,
#         email TEXT,
#         username TEXT,
#         registered_date TEXT,
#         phone TEXT,
#         picture TEXT);
#     """)

#     print("Table created successfully!")


# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')


def  create_spark_connection():
    s_conn = None

    try:
        # s_conn = SparkSession.builder.appName('SparkDataStreaming').config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1, org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1').config('spark.cassandra.connection.host', 'localhost').getOrCreate()
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.2.1, org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
            .getOrCreate()
        # Additional configurations for MongoDB connection
        s_conn.config('spark.mongodb.input.uri', 'mongodb://admin:admin@localhost:27017/streaming_data')  # can replace 'localhost' by 'mongodb' instead
        s_conn.config('spark.mongodb.output.uri', 'mongodb://admin:admin@localhost:your_port/streaming_data')

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:29092') \
            .option('subscribe', 'jobs') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


# def create_cassandra_connection():
#     try:
#         # connecting to the cassandra cluster
#         cluster = Cluster(['localhost'])

#         cas_session = cluster.connect()

#         return cas_session
#     except Exception as e:
#         logging.error(f"Could not create cassandra connection due to {e}")
#         return None

def create_mongodb_connection():
    try:
        client = MongoClient('localhost', 27017)
        # db = client['streaming_data']
        # job_collection = db['jobs']
        session = client['streaming_data']
        return session

    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_mongodb_collection(database, collection_name):
    try:
        collection = database[collection_name]

    except Exception as e:
        logging.error(f"Could not create MongoDB collection due to {e}")
  
def insert_single_document(collection, **kwargs):
    try:
        document = collection.insert_one(kwargs)
        logging.info(f"Inserted document with ID: {document.inserted_id}")
    except Exception as e:
        logging.error(f"Error inserting document: {e}")

  
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("job_name", StringType(), False),
        StructField("job_url", StringType(), False),
        StructField("type", StringType(), False),
        StructField("location", StringType(), False),
        StructField("company", StringType(), False),
        StructField("tag", StringType(), False),
        StructField("post_time", StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    # print("test" + spark_conn)

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df) 
        #session is database
        session = create_mongodb_connection()

        if session is not None:
            # create_keyspace(session)
            # create_table(session)
            create_mongodb_collection(session,'jobs')
            insert_single_document(session['jobs'])
        
            logging.info("Streaming is being started...")

            # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                    .option('checkpointLocation', '/tmp/checkpoint')
            #                    .option('keyspace', 'spark_streams')
            #                    .option('table', 'created_users')
                            #    .start())
            streaming_query = (selection_df.writeStream
                                .format("com.mongodb.spark.sql.DefaultSource")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('uri', 'mongodb://admin:admin@localhost:27017')
                                .option('database', 'spark_streams')
                                .option('collection', 'jobs  ')
                                .start())

            streaming_query.awaitTermination()