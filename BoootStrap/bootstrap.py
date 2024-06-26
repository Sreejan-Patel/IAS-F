import subprocess
from kafka import KafkaProducer, KafkaConsumer
import json, time
import sys

BOOTSTRAP_SERVER = 'localhost:9092'


def kafka_rpc(topic, request):
        # Sending the request
        request['timestamp'] = time.time()
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        producer.send(topic + 'In', json.dumps(request).encode('utf-8'))
        producer.flush()
        
        # Wait for the response
        consumer = KafkaConsumer(topic + "Out", bootstrap_servers=BOOTSTRAP_SERVER, auto_offset_reset='earliest')
        for msg in consumer:
            try:
                val = json.loads(msg.value)
                if val['request'] == request:
                    return val['result']
            except json.JSONDecodeError:
                pass
            except KeyError:
                pass

def start_logging():
    # Change Path
    subprocess.Popen(['python3', 'logger.py', BOOTSTRAP_SERVER], cwd='../Logging', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)
    subprocess.Popen(['python3', 'show_logs.py'], cwd='../Logging', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)

if __name__ == '__main__':
    BOOTSTRAP_SERVER = sys.argv[-1]

    start_logging()
    print('Logging started')

    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    log_message = {
        "level": 1,
        "service_name": "Bootstrap",
        "msg": "Bootstrap started"
    }
    logProducer.send('logs', value=log_message)

    # Change Path

    # NFS Serer Mount
    NFS_SERVER_PATH = "./nfs_server.sh"
    nfs_server = subprocess.run(['bash', NFS_SERVER_PATH])

    log_message = {
        "level": 1,
        "service_name": "Bootstrap",
        "msg": "NFS Server started"
    }

    logProducer.send('logs', value=log_message)

    # Change Path

    # # Stage 1
    agent_process = subprocess.Popen(['python3', 'agent.py', '0', BOOTSTRAP_SERVER], cwd='../agent', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)
    log_message = {
        "level": 1,
        "service_name": "Bootstrap",
        "msg": "Agent 0 started"
    }

    logProducer.send('logs', value=log_message)

    for process_config in json.load(open('boot_config.json'))['stage 1']:
        request = {'node_id': '0', 'method': 'start_process', 'args': {'config': process_config}}
        response = kafka_rpc('Agent', request)
        if response['status'] == 'success':
            print(process_config["name"] + ' Process started successfully')
            log_message = {
                "level": 1,
                "service_name": "Bootstrap",
                "msg": process_config["name"] + ' Process started successfully'
            }
            logProducer.send('logs', value=log_message)
        else:
            print(process_config["name"] + ' Process failed to start')
            log_message = {
                "level": 3,
                "service_name": "Bootstrap",
                "msg": process_config["name"] + ' Process failed to start'
            }
            logProducer.send('logs', value=log_message)
        time.sleep(5)


    ## Stage 2
    # Create a node
    # For each subsystem configuration, run the corresponding process.
    # Configuration = Node : Id, path, command json with three keys.
    for process_config in json.load(open('boot_config.json'))['stage 2']:
        new_node = kafka_rpc('NodeManager', {'method': 'create_node'})
        node_id = new_node['node_id']
        log_message = {
            "level": 1,
            "service_name": "Bootstrap",
            "msg": "Node " + str(node_id) + " allocated"
        }
        logProducer.send('logs', value=log_message)
        request = {'method': 'run_process_on_node', 'args': {'node_id': node_id, 'config': process_config}}
        response = kafka_rpc('NodeManager', request)
        log_message = {
            "level": 1,
            "service_name": "Bootstrap",
            "msg": process_config["name"] + ' Process started on Node ' + str(node_id)
        }
        logProducer.send('logs', value=log_message)
        print(response)