from kafka import KafkaProducer, KafkaConsumer
import json, paramiko, time, sys

SCRIPT_PATH = "./test_script.sh"
BOOTSTRAP_SERVER = 'localhost:9092'

def kafka_rpc(topic, request):
        # Sending the request
        print(BOOTSTRAP_SERVER)
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


class Node:
    def __init__(self, node_id, ip, username, password, vm_id):
        self.node_id = node_id
        self.ip = ip
        self.username = username
        self.password = password
        self.is_active = False
        self.vm_id = vm_id

    def activate_node(self):
        if(self.is_active == False):
            # Connect to node with ssh
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)

            # Type the command to run the initialization script
            sftp_client = ssh_client.open_sftp()
            sftp_client.put(SCRIPT_PATH, 'agent.sh')
            sftp_client.close()
            
            # Run the initialization script
            ssh_client.exec_command('bash agent.sh ' + str(self.node_id) + ' ' + BOOTSTRAP_SERVER)
            time.sleep(1)

            # Close the ssh connection
            ssh_client.close()
            self.is_active = True

    def reset_node(self):
        vm_response_reset = kafka_rpc("VmManager", {"method": "reset_vm", "args": {"vm_id": self.vm_id}})
        time.sleep(1)
        self.is_active = False
        self.activate_node()
        return vm_response_reset
    
    def deactivate_node(self):
        vm_response_deactivate = kafka_rpc("VmManager", {'method': 'remove_vm', 'args': {'vm_id': self.vm_id}}) ## properly written
        self.is_active = False
        self.vm_id = None
        return vm_response_deactivate
    
    def get_health(self):
        return kafka_rpc("Agent", {"method": "get_health", "node_id": str(self.node_id)})
    
    def run_process(self, process_config):
        # print(json.dumps({"node_id": str(self.node_id), "method": "start_process", "args": {'config': process_config }}, indent=4))
        process_details = kafka_rpc("Agent", {"method": "start_process", "node_id": str(self.node_id), "args": {'config': process_config }})
        if(process_details['status'] == 'success'):
            return True
        else:
            return False
        


## Have a list of corresponding node ids and their agents
class NodeManager:
    def __init__(self):
        self.nodes = {}
        logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def create_node(self):
        Vm_details = kafka_rpc("VmManager", {"method": "allocate_vm"})
        if(Vm_details['status'] == 'success'):
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "VM allocated -" + str(Vm_details['id'])
            }
            logProducer.send('logs', value=log_message)
            node = Node(len(self.nodes) + 1, Vm_details['ip'], Vm_details['username'], Vm_details['password'], Vm_details['id'])
            node.activate_node()
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Node activated -" + str(node.node_id)
            }
            logProducer.send('logs', value=log_message)
            self.nodes[node.node_id] = node
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Node added to nodes -" + str(node.node_id)
            }
            return {'status': 'success', "msg": "created a node", "node_id": node.node_id}
        else:
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "VM not allocated"
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'failure', "error": "No VMs available"}
            
    def remove_node(self, node_id):
        node_id = int(node_id)
        vm_remove_result = self.nodes[node_id].deactivate_node()
        if(vm_remove_result['status'] == 'success'):
            del self.nodes[node_id]
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Node removed -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'success', "msg": "Node removed"}
        else:
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "Node not removed"
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'failure', "error": "Node not removed"}
    
    def reset_node(self, node_id):
        node_id = int(node_id)
        response = self.nodes[node_id].reset_node()
        if(response['status'] == 'success'):
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Node reset -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'success', "msg": "Node reset"}
        else:
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "Node not reset"
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'failure', "error": "Node not reset"}
    
    def get_health(self, node_id):   ## ask agent of the corresponding nodes 
        node_id = int(node_id)
        response =  self.nodes[node_id].get_health()
        if(response['status'] == 'success'):
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Node health -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'success', "msg": "Node health", "health": response['health']}
        else:
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "Node health not available"
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'failure', "error": "Node health not available"}
        
    def run_process_on_node(self, node_id, process_config):
        node_id = int(node_id)
        if(node_id not in self.nodes):
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "Node not found -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {"status": 'failure', "error": "Node not found", "nodeid": node_id}
        
        sucess = self.nodes[node_id].run_process(process_config)
        if(sucess):
            log_message = {
                "level": 0,
                "service_name": "NodeManager",
                "msg": "Process started -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'success', "msg": "Process started", "nodeid": node_id, "process_config": process_config}
        else:
            log_message = {
                "level": 3,
                "service_name": "NodeManager",
                "msg": "Process failed to start -" + str(node_id)
            }
            logProducer.send('logs', value=log_message)
            return {'status': 'failure', "error": "Process failed to start", "nodeid": node_id, "process_config": process_config}
    
    
    
if __name__ == "__main__":
    BOOTSTRAP_SERVER = sys.argv[-1]
    # create a producer, log that node_manager has started.
    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    log_message = {
        "level": 1,
        "service_name": "NodeManager",
        "msg": "NodeManager started"
    }
    logProducer.send('logs', value=log_message)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

    # Start node_manager server
    node_manager = NodeManager()
    consumer = KafkaConsumer('NodeManagerIn', bootstrap_servers=BOOTSTRAP_SERVER)

    for msg in consumer:
        # Take input
        request = json.loads(msg.value.decode('utf-8'))
        log_message = {
            "level": 0,
            "service_name": "NodeManager",
            "msg": "Received request -" + request['method']
        }
        logProducer.send('logs', value=log_message)
        
        # Process RPC request
        if(request['method'] == 'create_node'):
            result = node_manager.create_node()
            # if(result['status'] == 'success'):
            #     node_manager.nodes[result['node_id']].activate_node()
        elif(request['method'] == 'remove_node'):
            result = node_manager.remove_node(request['args']['node_id'])
        elif(request['method'] == 'reset_node'):
            result = node_manager.reset_node(request['args']['node_id'])
        elif(request['method'] == 'get_health'):
            result = node_manager.get_health(request['args']['node_id'])
        elif(request['method'] == 'run_process_on_node'):
            result = node_manager.run_process_on_node(request['args']['node_id'], request['args']['config'])
        else:
            result = {"error": "method not found"}
        
        # Send output
        response = {"request": request, "result": result}
        log_message = {
            "level": 1,
            "service_name": "NodeManager",
            "msg": "Sending response -" + request['method']
        }
        logProducer.send('logs', value=log_message)
        producer.send("NodeManagerOut", json.dumps(response).encode('utf-8'))

   