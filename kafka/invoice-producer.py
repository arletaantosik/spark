#install confluent-kafka (package) in pycharm
# takes each line from a file, convert it in key-value message and send it to kafka
from confluent_kafka import Producer
import json
import time


class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        self.conf = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092', #location of the kafka cluster + port in which kafka cluster is listening to
                     'security.protocol': 'SASL_SSL', # only applications that have credential to send data to a particular topic should be allowed
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': 'M4DVA3BPMOUH4RSN',
                     'sasl.password': '2ONBxDYYc/YSg1+ZOdnsjFw3UxeSzjAV5vi26a8rvuNAyYGWB3EgjjAOpOQFnIoL',
                     'client.id': "arleta-laptop"}

    def delivery_callback(self, err, msg): #2nd argument - error, 3rd message
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8') #because string is converted to binary format when it goes to kafka and then we have to decode binary
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"] #convert to json object, so we can pick "InvoiceNumber"
            print(f"Produced event to : key = {key} value = {invoice_id}")

    def produce_invoices(self, producer):
        with open("data/invoices.json") as lines: #open json file
            for line in lines: #loop line
                invoice = json.loads(line) #convert line in json object -> equals to python dictonary object
                store_id = invoice["StoreID"] #store_id as key, invoice as a message -> send it to kafka
                producer.produce(self.topic, key=store_id, value=json.dumps(invoice), callback=self.delivery_callback) #callback - message to producer (if the message passed or failed)
                time.sleep(0.5)
                producer.poll(1) #timeout in seconds; after sending data to kafka broker, poll will make sure that .delivery_callback is called for each message

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush()


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()
