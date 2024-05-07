# from flask import Flask, jsonify
import time
import json
import redis
# from flask_cors import CORS , cross_origin
from confluent_kafka import Consumer , Producer
import sys

# Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)

# Constants
SERVICE_REGISTRY_KEY = 'service_registry'
kafka_broker = '10.1.36.50:9092'

kafka_nm_topic = 'AgentIn'

producer = Producer({'bootstrap.servers': kafka_broker})

def send_down_update(process):
    message = {
        'method' : 'restart_service' , 
        'node_id' : process['node_id'] , 
        'process_id' : process['process_id'],
        'process_address' : process['address'],
        'config' : process['capabilities']
    }
    print(message)
    producer.produce(kafka_nm_topic , json.dumps(message).encode('utf-8'))
    producer.flush()
    print(f'service up request sent : {process["process_id"]}')




class RegistryServiceManager:
    def __init__(self):
        pass


    def register_service(self, process_id, node_id, service_address, service_capabilities, topic):
        service_data = {
            'node_id': node_id,
            'process_id' : process_id , 
            'address': service_address,
            'capabilities': service_capabilities,
            'topic' : topic,
            'fail_count' : 0 , 
            'health': 'H'
        }
        r.hset(SERVICE_REGISTRY_KEY, process_id, json.dumps(service_data))

    def discover_services(self, service_name=None):
        if service_name:
            service_data = r.hget(SERVICE_REGISTRY_KEY, service_name)
            if service_data:
                service_data = json.loads(service_data.decode('utf-8'))
                health_status = self.get_service_health(service_name)
                service_data['health'] = health_status  # Add health status to service data
                return service_data
            else:
                return None
        else:
            services = {}
            for key in r.hkeys(SERVICE_REGISTRY_KEY):
                service_data = r.hget(SERVICE_REGISTRY_KEY, key)
                service_data = json.loads(service_data.decode('utf-8'))
                service_name = key.decode('utf-8')
                health_status = self.get_service_health(service_name)
                service_data['health'] = health_status  # Add health status to service data
                services[service_name] = service_data
            return services

    def update_service_health(self, service_name):
        temp = json.loads(r.hget(SERVICE_REGISTRY_KEY , service_name).decode('utf-8'))
        
        if 'fail_count' not in temp:
            temp['fail_count'] = 0
        temp['fail_count'] += 1

        if 'health' not in temp:
            temp['health'] = 'H'
        if temp['health'] == 'U':
            return
        if temp['fail_count'] >= 2:
            temp['health'] = 'U'
            send_down_update(temp)
            temp['health'] = 'H'
            temp['fail_count']   = 0
        r.hset(SERVICE_REGISTRY_KEY, service_name, json.dumps(temp))

    def restore_service_health(self , service_name):
        temp = json.loads(r.hget(SERVICE_REGISTRY_KEY , service_name).decode('utf-8'))
        if 'fail_count' not in temp:
            temp['fail_count'] = 0
        if 'health' not in temp:
            temp['health'] = 'H'
        temp['health'] = 'H'
        temp['fail_count'] = 0
        r.hset(SERVICE_REGISTRY_KEY , service_name , json.dumps(temp))
    


    def get_service_health(self, service_name):
        status = r.hget(SERVICE_REGISTRY_KEY, service_name)
        if status is None:
            return None
        status = json.loads(status.decode('utf-8'))
        if 'health' not in status:
            status['health'] = 'H'
        return status['health'] if status else None

registry = RegistryServiceManager()



consumer = Consumer({
    'bootstrap.servers': kafka_broker,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

# consumer = KafkaConsumer('PingIn' , bootstrap_servers = kafka_broker)

kafka_topic = 'PingIn'


consumer.subscribe([kafka_topic])
# consumer.subscribe([kafka_add])


ping_topics  = [
 
]
ping_ids = {

}
missed_count = 0
batch_size = 50

def check_health():
    global missed_count

    for key in r.hkeys(SERVICE_REGISTRY_KEY):
        service_data = r.hget(SERVICE_REGISTRY_KEY, key)
        service_data = json.loads(service_data.decode('utf-8'))
        print(service_data)
        ping_topics.append(service_data['topic'])
        ping_ids[service_data['topic']] = {
            'process_id' : service_data['process_id'], 
            'node_id' : service_data['node_id']
        }
          


    while True:
        for topic in ping_topics:
            message_ping = {
                'method' : 'ping' , 
                'beat' : 'ping',
                'node_id' : ping_ids[topic]['node_id'],
                'process_id' : ping_ids[topic]['process_id']
                }
            producer.produce(topic , json.dumps(message_ping).encode('utf-8'))
            producer.flush()
        time.sleep(12.0)
        # msg = consumer.poll(1.0)
        batch = consumer.consume(batch_size , timeout = 1.0)
        # print(len(batch))
        # for m in batch:
        #     print(m)
        # continue
        up_services = []
        all_services = registry.discover_services()


        for msg in batch:
            if msg is None:
                missed_count += 1
                # print(f'message not receivec : {missed_count}')
            elif not msg.error():
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    if(message['method'] == 'pingback'):
                    # if 'LIVE' in message.values():
                        if message['process_id'] not in all_services:
                            print('here')
                            registry.register_service(message['process_id'] , '' , {})
                        process_name = message.get('process_id')
                        missed_count = 0
                        if process_name not in up_services: 
                            up_services.append(process_name)
                    elif(message['method'] == 'register'):
                        # 'service_type'
                        print(f'registering')
                        print(msg.topic())
                        message_value = json.loads(msg.value().decode('utf-8'))
                        print(message_value['process_id'])
                        if message_value['topic'] not in ping_topics:
                            ping_topics.append(message_value['topic'])
                        ping_ids[message_value['topic']] = {
                            'process_id' : message_value['process_id'] ,    
                            'node_id' : message_value['node_id']

                        }
        
                        registry.register_service(message_value['process_id'] , message_value['node_id'], message_value['service_address'] , message_value['config'] , message_value['topic'])
                        print('new service registered')
                    elif(message['method'] == 'restart_service_back'):
                        if(message['status'] == 'success'):
                            print(f'process with process_id = {message["process_id"]}  has been restarted' )
                        else:
                            print(f'process with process_id = {message["process_id"]}  not restarted' )


                except Exception as e:
                    print(f'decoding error for message {e}')
            else:
                print('error in msg')
        print('updating')
        print(up_services)
        for service in all_services:
            if service not in up_services:
                registry.update_service_health(service)
            else:
                print(service)
                registry.restore_service_health(service)


        

def sigterm_handler(signum, frame):
    print('SIGTERM received, exiting...')
    sys.exit(0)

if __name__ == '__main__':
    print('main')
    # signal.signal(signal.SIGTERM, sigterm_handler)
    # flask_thread = Thread(target=app.run , kwargs={'debug' : True})
    # flask_thread.start()
    # app.run(debug=True)
    check_health()
    # print('after')
    # flask_thread.join()
    print('finish')  
