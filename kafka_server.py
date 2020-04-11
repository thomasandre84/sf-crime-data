import producer_server

TOPIC = "police.department.service.calls"


def run_kafka_server():
	# get the json file path
    input_file = "./police-department-calls-for-service.json"

    # fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC,
        bootstrap_servers="localhost:9092",
        client_id="some_id"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
