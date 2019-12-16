from kafka import KafkaProducer
import  time

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             retries=1, batch_size=16384,
                             linger_ms=1, buffer_memory=33554432,
                             value_serializer=lambda x: x.encode('utf-8'))

    topicName = "appointments"
    with open('../data/data.txt') as file:
        for line in file:
            producer.send(topicName, line)
            time.sleep(0.001)