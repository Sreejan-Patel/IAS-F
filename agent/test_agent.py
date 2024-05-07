from kafka import KafkaProducer, KafkaConsumer
import json, time
import sys


requests = [
    {
        'node_id': '0',
        'method': 'start_process',
        'args': {
            'config': {
                'name': 'install',
                'path': '/home/balaramakrishna/Documents/4.2/IAS/Last/IAS-F/NodeManager',
                'command': '/home/balaramakrishna/anaconda3/envs/ias/bin/python node_manager.py',
                },
            'topic': 'NodeManagerIn'
            },
    },
    # {
    #     'node_id': '0',
    #     'method': 'kill_process',
    #     'args': {
    #         'process_id': '628796b0-7c35-4d03-b504-75f72876a54e'
    #     },
    # },
    # {
    #     'node_id': '0',
    #     'method': 'reset_process',
    #     'args': {
    #         'process_id': '628796b0-7c35-4d03-b504-75f72876a54e'
    #         },
    # },
    # {
    #     'node_id': '0',
    #     'method': 'get_health',
    # },
    # {
    #     'node_id': '0',
    #     'method': 'get_processes',
    # },
]


if __name__ == "__main__":
    BOOTSTRAP_SERVER = sys.argv[1]
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    for request in requests:
        # Sending the request
        request['timestamp'] = time.time()
        producer.send("AgentIn", json.dumps(request).encode('utf-8'))
        
        # Wait for the response
        consumer = KafkaConsumer("AgentOut", bootstrap_servers=BOOTSTRAP_SERVER, auto_offset_reset='earliest')
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