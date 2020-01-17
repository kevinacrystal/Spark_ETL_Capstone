from kafka import KafkaProducer
import requests
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
spark = SparkSession.builder.getOrCreate()


# tab-separated files / topics
url_1 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partOne.txt'
url_2 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partTwo.txt'
url_3 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partThree.txt'
url_4 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partFour.txt'
url_5 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/insurance.txt'
url_6 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/PlanAttributes.csv'
topic_1 = 'BenefitsCostSharing_partOne'
topic_2 = 'BenefitsCostSharing_partTwo'
topic_3 = 'BenefitsCostSharing_partThree'
topic_4 = 'BenefitsCostSharing_partFour'
topic_5 = 'Insurance'
topic_6 = 'PlanAttributes'

# csv files / topics
url_7 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv'
url_8 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/ServiceArea.csv'
topic_7 = 'Network'
topic_8 = 'ServiceArea'


def kafka_producer(inpath, topic):
    # extract data from url and create a list of lines
    r=requests.get(inpath)
    text = r.text
    data_list = [data for data in text.splitlines()]
    print(f'Extracted {len(data_list)} lines from {inpath}')
    
    # instantiate a producer and send the data to a topic one line at a time
    producer =  KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v:v.encode('utf-8'))    
    for line in data_list:
        producer.send(topic, line)
    producer.flush()
    print(f'Wrote data to Kafka topic: {topic}\n')


def make_df_from_topic(topic, delimiter): 
    # consume the data from kafka and return a streaming dataframe
    raw_kafka_df = spark.readStream\
                    .format('kafka')\
                    .option('kafka.bootstrap.servers', 'localhost:9092')\
                    .option('subscribe', topic)\
                    .option('startingOffsets', 'earliest')\
                    .load()  
    
    # convert the data from byte back into readable text
    # and return a new 
    kafka_value_df = raw_kafka_df.selectExpr('CAST(value AS STRING)')
    
    aggDF = kafka_value_df.writeStream\
                            .queryName('aggregates')\
                            .format('memory')\
                            .start()
    aggDF.awaitTermination(10)
    
    value_df = spark.sql(f'select * from aggregates')
       
    value_rdd = value_df.rdd.map(lambda i: i['value'].split(delimiter))
    header = value_rdd.first()
    df_with_schema = value_rdd.filter(lambda row: row!=header).toDF(header)
    
    return df_with_schema          
    
# Load dataframe into MongoDB
def load_into_mongo(df_schema, target_collection):
    df_schema.write\
        .format('mongo') \
        .mode('append') \
        .option('database', 'health_insurance_marketplace_data') \
        .option('collection', target_collection) \
        .option('uri', 'mongodb://localhost')\
        .save()
    print(f'\nWrote data to Mongo collection: {target_collection}')
        
def main(inpath, topic, delimiter, target_collection):
    kafka_producer(inpath, topic)
    df_schema = make_df_from_topic(topic, delimiter)
    load_into_mongo(df_schema, target_collection)

if __name__=='__main__':
    main()
