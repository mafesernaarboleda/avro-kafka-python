from kafka import *
import io
import avro.schema
import avro.datafile
import avro.io
import time
import sys

kafka_client = KafkaClient('52.2.239.144:9092')
producer = KeyedProducer(kafka_client)
schema_path = "RuleMessage.avsc"
schema = avro.schema.parse(open(schema_path).read())


def message_serializer(station, model, iodata, value, connection):
    raw_bytes = None
    try:
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write({
            "station": station,
            "model": model,
            "io": iodata,
            "timestamp": int(round(time.time() * 1000)),
            "value": value,
            "connection": connection
        },
            encoder)
        raw_bytes = bytes_writer.getvalue()
    except:
        print("Error serializer data", sys.exc_info()[0])
    return raw_bytes


def send_message_producer(topic, raw_bytes, station):
    try:
        producer.send_messages(topic, station, raw_bytes)
    except:
        print("Error send message kafka", sys.exc_info()[0])


topic = "events"
messageToSend = message_serializer("12", "32", "e", 1.3, True)
raw_bytes = messageToSend
if raw_bytes is not None:
    send_message_producer(topic, raw_bytes, "idestacion")
