from kafka import KafkaProducer
from googlefinance import getQuotes

from kafka.errors import (
    KafkaError,
    KafkaTimeoutError
)

import logging
import json
import time
import schedule
import argparse
import atexit

class data_producing():

	def __init__(self, symbol, kafka_broker, topic):
		self.symbol = symbol
		self.kafka_broker = kafka_broker
		self.topic = topic
		self.producer = KafkaProducer(bootstrap_servers = kafka_broker)
		logging.basicConfig(format = '%(asctime)-15s %(message)s')
		self.logger = logging.getLogger('stream_process')
		self.logger.setLevel(logging.INFO)
		
	def fetch_stock_data(self):
		try:
			stock_data = json.dumps(getQuotes(self.symbol))
			self.logger.info('Fetched data for %s' % self.symbol)
			return stock_data
		except Exception:
			self.logger.warn('Failed to fetch data for %s' % self.symbol)

	def send_stock_data(self):
		try:
			stock_data = self.fetch_stock_data()
			self.producer.send(topic = self.topic, value = stock_data, timestamp_ms = time.time())
			self.logger.info('Sent stock data for %s, stock data is: %s' % (self.symbol, stock_data))
		except KafkaTimeoutError as timeout_error:
			self.logger.warn('Failed to send stock data for %s to kafka, caused by %s' % (self.symbol, timeout_error.message))

	def schedule_deliver(self):
		schedule.every(1).second.do(self.send_stock_data)
		while True:
			schedule.run_pending()
			time.sleep(1)

	def shutdown_hook(self):
	    try:
	        self.producer.flush(10)
	        self.logger.info('Finished flushing pending messages to kafka')
	    except KafkaError as kafka_error:
	        self.logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
	    finally:
	        try:
	            self.producer.close(10)
	            self.logger.info('Closed kafka connection')
	        except KafkaError as kafka_error:
	            self.logger.warn('Failed to close kafka connection, caused by: %s', kafka_error.message)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help = 'the stock symbol, such as AAPL')
	parser.add_argument('kafka_broker', help = 'the path of kafka broker')
	parser.add_argument('topic', help = 'the kafka topic to write to')

	args = parser.parse_args()
	symbol = args.symbol
	kafka_broker = args.kafka_broker
	topic = args.topic

	dp = data_producing(symbol, kafka_broker, topic)
	atexit.register(dp.shutdown_hook)
	dp.schedule_deliver()






