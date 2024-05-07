from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import subprocess
import uuid
import psutil
import os



BOOTSTRAP_SERVER = 'localhost:9092'


# maintain the list of all processes
class Process:
    def __init__(self, name, path, command):
        self.name = name
        self.command = command
        self.path = path
        self.process = None
        self.process_id = str(uuid.uuid4())  # Generate unique ID for each process

    def start(self):
        cdCommand = "cd " + self.path
        self.command = cdCommand + " && " + self.command + " " + BOOTSTRAP_SERVER
        self.process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return self.process_id

    def kill(self):
        if self.process:
            self.process.kill()
            self.process = None

    def reset(self):
        if self.process:
            self.kill()
            self.start()

    def status(self):
        if self.process and self.process.poll() is None:
            return "Running"
        else:
            return "Not running or not started"

class Processes:
    def __init__(self):
        self.processes = {}

    def start_process(self, name, path, command):
        new_process = Process(name, path, command)
        process_id = new_process.start()
        self.processes[process_id] = new_process
        return process_id

    def kill_process(self, process_id):
        if process_id in self.processes:
            self.processes[process_id].kill()
            # Remove the process from the list
            del self.processes[process_id]

    def reset_process(self, process_id):
        if process_id in self.processes:
            self.processes[process_id].reset()

    def get_processes(self):
        return {pid: proc.status() for pid, proc in self.processes.items()}

class Agent:
    def __init__(self, node_id):
        self.node_id = node_id
        self.processes = Processes()
        self.logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def start_process(self, process_config):
        name = process_config['name']
        command = process_config['command']
        path = process_config['path']

        if not os.path.exists(path):
            log_message = {
                "level": 2,
                "service_name": "Agent" + self.node_id,
                "msg": f"Path '{path}' does not exist."
            }
            self.logProducer.send("logs", value=log_message)
            return {'method': 'start_process', 'status': 'error', 'message': f"Path '{path}' does not exist."}

        log_message = {
            "level": 0,
            "service_name": "Agent" + self.node_id,
            "msg": f"Starting process '{name}'"
        }
        self.logProducer.send("logs", value=log_message)
        return {'method': 'start_process', 'process_id': self.processes.start_process(name, path,command), 'status': 'success'}

    def kill_process(self, process_id):
        self.processes.kill_process(process_id)
        log_message = {
            "level": 0,
            "service_name": "Agent" + self.node_id,
            "msg": f"Killing process '{process_id}'"
        }
        self.logProducer.send("logs", value=log_message)
        return {'method': 'kill_process', 'process_id': process_id, 'status': 'success'}

    def reset_process(self, process_id):
        self.processes.reset_process(process_id)
        log_message = {
            "level": 0,
            "service_name": "Agent" + self.node_id,
            "msg": f"Resetting process '{process_id}'"
        }
        self.logProducer.send("logs", value=log_message)
        return {'method': 'reset_process', 'process_id': process_id, 'status': 'success'}

    def get_processes(self):
        log_message = {
            "level": 0,
            "service_name": "Agent" + self.node_id,
            "msg": "Getting processes"
        }
        self.logProducer.send("logs", value=log_message)
        return {'method': 'get_processes', 'processes': self.processes.get_processes(), 'status': 'success'}

    def get_health(self):
        # Improved health check using psutil
        free_cores = psutil.cpu_count(logical=True)
        free_memory = psutil.virtual_memory().available
        free_memory_gb = round(free_memory / (1024**3), 2)  # Convert bytes to GB
        
        log_message = {
            "level": 0,
            "service_name": "Agent" + self.node_id,
            "msg": "Getting health"
        }
        self.logProducer.send("logs", value=log_message)

        # Return both CPU and memory info
        return {
            'method': 'get_health',
            'free_cores': free_cores,
            'free_memory_gb': free_memory_gb,
            'status': 'success'
        }


if __name__ == "__main__":
    # get the node_id.
    node_id = sys.argv[1]
    BOOTSTRAP_SERVER = sys.argv[-1]
    
    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    log_message = {
        "level": 0,
        "service_name": "Agent" + str(node_id),
        "msg": "Agent has been started"
    }
    logProducer.send("logs", value=log_message)
    # create a producer, log that agent has started.
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    # producer.flush()

    # Start agent server
    agent = Agent(node_id)
    consumer = KafkaConsumer('AgentIn', bootstrap_servers=BOOTSTRAP_SERVER)
   
    for msg in consumer:
        request = json.loads(msg.value)
        if(request['node_id'] == node_id):
            log_message = {
                "level": 0,
                "service_name": "Agent" + str(node_id,)
                "msg": "Request received"
            }
            logProducer.send("logs", value=log_message)
            
            # Process RPC request
            if(request['method'] == 'start_process'):
                result = agent.start_process(request['args']['config'])
            elif(request['method'] == 'kill_process'):
                result = agent.kill_process(request['args']['process_id'])
            elif(request['method'] == 'reset_process'):
                result = agent.reset_process(request['args']['process_id'])
            elif(request['method'] == 'get_processes'):
                result = agent.get_processes()
            elif(request['method'] == 'get_health'):
                result = agent.get_health()
            else:
                result = {'error': 'Invalid method'}
                print("Invalid method ", request['method'])
            
            # Send output
            response = {"request": request, "result": result}
            log_message = {
                "level": 0,
                "service_name": "Agent" + str(node_id),
                "msg": "Response sent - Agent" + str(node_id)
            }
            logProducer.send("logs", value=log_message)
            producer.send('AgentOut', json.dumps(response).encode('utf-8'))