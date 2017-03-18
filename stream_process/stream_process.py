# - read from kafka
# - do processing using spark
# - write to kafka

from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka.errors import KafkaError, KafkaTimeoutError

import sys
import json
import logging
import time
import atexit

class stream_processer():

	def __init__(self, receiveTopic, sendTopic, kafka_broker):
		self.sc = SparkContext('local[2]', 'StockAveragePrice')
		self.ssc = StreamingContext(self.sc, 5)
		self.receiveTopic = receiveTopic
		self.sendTopic = sendTopic
		self.kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)
		self.directKafkaStream = KafkaUtils.createDirectStream(self.ssc, [receiveTopic], {'metadata.broker.list': kafka_broker})

		logging.basicConfig(format = '%(asctime)-15s %(message)s')
		self.logger = logging.getLogger('stream_process')
		self.logger.setLevel(logging.INFO)


	def shutdown_hook(self):
	    try:
	        self.kafka_producer.flush(10)
	        self.logger.info('Finished flushing pending messages to kafka')
	    except KafkaError as kafka_error:
	        self.logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
	    finally:
	        try:
	            self.kafka_producer.close(10)
	            self.logger.info('Closed kafka connection')
	        except KafkaError as kafka_error:
	            self.logger.warn('Failed to close kafka connection, caused by: %s', kafka_error.message)

	def process_stream(self, DStream):
	    def send_to_kafka(rdd):
	        results = rdd.collect()
	        for r in results:
	            data = json.dumps(
	                {
	                    'symbol': r[0],
	                    'timestamp': time.time(),
	                    'average': r[1]
	                }
	            )
	            try:
	                self.logger.info('Sending average price %s to kafka' % data)
	                self.kafka_producer.send(self.sendTopic, value=data)
	            except KafkaError as error:
	                self.logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

	    def pair(data):
	        record = json.loads(data[1].decode('utf-8'))[0]
	        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1)

	    DStream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
	           .map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)


	def run(self):
		self.process_stream(self.directKafkaStream)
		self.ssc.start()
		self.ssc.awaitTermination()


if __name__ == '__main__':
	receiveTopic, sendTopic, kafka_broker = sys.argv[1:]

	sp = stream_processer(receiveTopic, sendTopic, kafka_broker)
	atexit.register(sp.shutdown_hook)
	sp.run()






