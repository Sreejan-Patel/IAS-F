import json, time
import paramiko
import threading
import sys
from kafka import KafkaConsumer, KafkaProducer

SCRIPT_PATH = './bootstrap.sh'
VM_LIST_PATH = './vm_list.json'
GET_HEALTH_SCRIPT_PATH = './get_health.py'
BOOTSTRAP_SERVER = 'localhost:9092'

class VM:
    def __init__(self, ID, ip, username, password):
        self.id = ID
        self.ip = ip
        self.username = username
        self.password = password
        self.is_active = False
        self.logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def activate_vm(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(SCRIPT_PATH, 'nfs_client.sh')
        sftp_client.close()
        ssh_client.exec_command('bash nfs_client.sh')
        time.sleep(1)
        print("VM activated")
        ssh_client.close()
        self.is_active = True
        log_message = {
            "level": 0,
            "service_name": "VMManager",
            "msg": f"VM activated: {self.ip}"
        }
        self.logProducer.send('logs', value=log_message)

    def deactivate_vm(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)
        ssh_client.exec_command('echo ' + self.password + ' | ' + 'sudo -S killall -9 -u ' + self.username)
        time.sleep(1)
        ssh_client.close()
        self.is_active = False
        log_message = {
            "level": 0,
            "service_name": "VMManager",
            "msg": f"VM deactivated: {self.ip}"
        }
        self.logProducer.send('logs', value=log_message)

    def get_health(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(GET_HEALTH_SCRIPT_PATH, 'get_health.py')
        sftp_client.close()
        _, stdout, _ = ssh_client.exec_command('python3 get_health.py')
        output = stdout.read().decode()
        cpu_usage, memory_free = map(float, output.strip().split(','))
        ssh_client.close()
        log_message = {
            "level": 0,
            "service_name": "VMManager",
            "msg": f"Health stats retrieved for VM: {self.ip}"
        }
        self.logProducer.send('logs', value=log_message)
        return cpu_usage, memory_free

    def to_dict(self):
        return {"id": self.id, "ip": self.ip, "username": self.username, "is_active": self.is_active}


class VMManager:
    def __init__(self, vm_list_path):
        self.vm_list_path = vm_list_path
        self.vms = self.load_vms()
        self.lock = threading.Lock()
        self.logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def load_vms(self):
        with open(self.vm_list_path, 'r') as file:
            vm_data = json.load(file)
            return [VM(id, vm['ip'], vm['username'], vm['password']) for id, vm in enumerate(vm_data, start=1)]

    def allocate_vm(self):
        with self.lock:
            for vm in self.vms:
                if not vm.is_active:
                    vm.activate_vm()
                    log_message = {
                        "level": 0,
                        "service_name": "VMManager",
                        "msg": f"VM allocated: {vm.ip}"
                    }
                    self.logProducer.send('logs', value=log_message)
                    return {"msg": "VM allocated", "id": vm.id, "ip": vm.ip, "username": vm.username, "password": vm.password, "status": "success"}
            
            # Assess health of all active VMs
            log_message = {
                "level": 1,
                "service_name": "VMManager",
                "msg": "Assessing health of active VMs"
            }
            self.logProducer.send('logs', value=log_message)
            health_stats = [(vm.get_health(), vm) for vm in self.vms if vm.is_active]
            if health_stats:
                # Select the VM with the lowest CPU usage and most free memory
                best_vm = min(health_stats, key=lambda x: (x[0][0], -x[0][1]))[1]
                log_message = {
                    "level": 0,
                    "service_name": "VMManager",
                    "msg": f"Best VM selected: {best_vm.ip}"
                }
                self.logProducer.send('logs', value=log_message)
                return {"msg": "VM allocated", "id": best_vm.id, "ip": best_vm.ip, "username": best_vm.username, "password": best_vm.password}

            log_message = {
                "level": 2,
                "service_name": "VMManager",
                "msg": "No inactive VM available"
            }
            self.logProducer.send('logs', value=log_message)
            return {"msg": "No inactive VM available"}
    
    def get_vms(self):
        with self.lock:
            log_message = {
                "level": 0,
                "service_name": "VMManager",
                "msg": "Retrieving VMs"
            }
            self.logProducer.send('logs', value=log_message)
            return {"vms": [vm.to_dict() for vm in self.vms], "status": "success"}
    
    def remove_vm(self, vm_id):
        with self.lock:
            for vm in self.vms:
                if vm.id == int(vm_id) and vm.is_active:
                    log_message = {
                        "level": 0,
                        "service_name": "VMManager",
                        "msg": f"Removing VM: {vm.ip}"
                    }
                    self.logProducer.send('logs', value=log_message)
                    vm.is_active = False
                    vm.deactivate_vm()
                    return {"msg": "VM removed", "status": "success"}
            log_message = {
                "level": 2,
                "service_name": "VMManager",
                "msg": "VM not found"
            }
            self.logProducer.send('logs', value=log_message)
            return {"msg": "VM not found", "status": "failure"}

    def reset_vm(self, vm_id):
        with self.lock:
            for vm in self.vms:
                if vm.id == int(vm_id) and vm.is_active:
                    log_message = {
                        "level": 0,
                        "service_name": "VMManager",
                        "msg": f"Resetting VM: {vm.ip}"
                    }
                    self.logProducer.send('logs', value=log_message)
                    vm.is_active = False
                    vm.deactivate_vm()
                    vm.activate_vm()
                    return {"msg": "VM reset", "status": "success"}
            log_message = {
                "level": 2,
                "service_name": "VMManager",
                "msg": "VM not found"
            }
            self.logProducer.send('logs', value=log_message)
            return {"msg": "VM not found", "status": "failure"}

    def get_health_all(self):
        with self.lock:
            log_message = {
                "level": 0,
                "service_name": "VMManager",
                "msg": "Retrieving health stats for all VMs"
            }
            self.logProducer.send('logs', value=log_message)
            health_stats = [(vm.get_health(), vm) for vm in self.vms if vm.is_active]
            return [{"ip": vm.ip, "cpu_usage": health[0], "memory_free": health[1]} for health, vm in health_stats]

    def get_health(self, vm_id):
        with self.lock:
            for vm in self.vms:
                if vm.id == int(vm_id):
                    log_message = {
                        "level": 0,
                        "service_name": "VMManager",
                        "msg": f"Retrieving health for VM: {vm.ip}"
                    }
                    self.logProducer.send('logs', value=log_message)
                    return {"ip": vm.ip, "cpu_usage": vm.get_health()[0], "memory_free": vm.get_health()[1], "status": "success"}
            log_message = {
                "level": 2,
                "service_name": "VMManager",
                "msg": "VM not found"
            }
            self.logProducer.send('logs', value=log_message)
            return {"msg": "VM not found", "status": "failure"}


# Usage
if __name__ == '__main__':
    BOOTSTRAP_SERVER = sys.argv[-1]
    
    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    log_message = {
        "level": 0,
        "service_name": "VMManager",
        "msg": "VM Manager started"
    }
    
    logProducer.send('logs', value=log_message)
    # create a producer, log that vm_manager has started.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Start vm_manager server
    vm_manager = VMManager(VM_LIST_PATH)
    consumer = KafkaConsumer('VmManagerIn', bootstrap_servers='localhost:9092')
    
    for msg in consumer:
        # Parse the request
        request = json.loads(msg.value)
        log_message = {
            "level": 0,
            "service_name": "VMManager",
            "msg": f"Received request: {request}"
        }
        logProducer.send('logs', value=log_message)
        
        # Process RPC request
        if(request['method'] == 'allocate_vm'):
            result = vm_manager.allocate_vm()
        elif(request['method'] == 'remove_vm'):
            result = vm_manager.remove_vm(request['args']['vm_id'])
        elif(request['method'] == 'reset_vm'):
            result = vm_manager.reset_vm(request['args']['vm_id'])
        elif(request['method'] == 'get_vms'):
            result = vm_manager.get_vms()
        elif(request['method'] == 'get_health'):
            result = vm_manager.get_health(request['args']['vm_id'])
        else:
            result = {'error': 'Invalid method'}
            print("Invalid method ", request['method'])
            
        # Send the response
        response = {"request": request, "result": result}
        log_message = {
            "level": 0,
            "service_name": "VMManager",
            "msg": f"Sending response: {response}"
        }
        logProducer.send('logs', value=log_message)
        producer.send("VmManagerOut", json.dumps(response).encode('utf-8'))

