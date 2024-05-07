from kafka import KafkaProducer, KafkaConsumer
import json, time


requests = [
    # {
    #     'method': 'create_node',
    # },
    # {
    #     'method': 'remove_node',
    #     'args': {
    #         'node_id': '1'
    #         },
    # },
    {
        'method': 'reset_node',
        'args': {
            'node_id': '1'
            },
    },
    # {
    #     'method': 'get_health',
    #     'args': {
    #         'node_id': '1'
    #         },
    # },
    # {
    #     'method': 'run_process_on_node',
    #     'args': {
    #         'node_id': '1',
    #         'config': {
    #             'name': 'install',
    #             'path': '.',
    #             'command': 'bash install.sh',
    #             }
    #         },
    # },
]


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='10.1.36.50:9092')
    for request in requests:
        # Sending the request
        request['timestamp'] = time.time()
        producer.send("NodeManagerIn", json.dumps(request).encode('utf-8'))
        
        # Wait for the response
        consumer = KafkaConsumer("NodeManagerOut", bootstrap_servers='10.1.36.50:9092', auto_offset_reset='earliest')
        for msg in consumer:
            try:
                val = json.loads(msg.value)
                if val['request'] == request:
                    print(val)
                    break
            except json.JSONDecodeError:
                pass
            except KeyError:
                pass
    producer.flush()