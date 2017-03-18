# Kafka

## simple_data_producer.py
Implemented a kafka producer, grabbed data of one stock every second from Google finance and sent to Kafka

### Run code
```sh
python simple_data_producer.py AAPL stock-analyzer 192.168.99.100:9092
```


## flask_data_producer.py
Implemented a kafka producer, grabbed data of one stock every second from Google finance and sent to Kafka.
The stock information grabbed can be dynamically added or deleted via HTTP requests.

### Run code 
```sh
export ENV_CONFIG_FILE=`pwd`/config/dev.cfg
python flask_data_producer.py
```