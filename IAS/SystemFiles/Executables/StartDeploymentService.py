#Importing the necessary modules and variables
import os
import json
import subprocess
from Repository import RepositoryDirectory, PythonPath
from Logging import logger

# Path to the JSON files folder
JSON_FILES = os.path.join(os.path.join(RepositoryDirectory, 'SystemFiles'), 'JSONs')

StartDeploymentServiceJSONFile = 'StartDeploymentService.json'

# Check if the StartDeploymentServiceJSONFile exists inside the JSON_FILES folder
if not os.path.exists(os.path.join(JSON_FILES, StartDeploymentServiceJSONFile)):
    logger.error('{} file does not exist inside the JSON_FILES folder'.format(StartDeploymentServiceJSONFile))
    raise FileNotFoundError('{} file does not exist inside the JSON_FILES folder'.format(StartDeploymentServiceJSONFile))

# Reading the StartDeploymentServiceJSONFile file, which contains the hostname and port number for the DeploymentService to run
with open(os.path.join(JSON_FILES, StartDeploymentServiceJSONFile), 'r') as f:
    
    logger.info('Reading {} file',format(StartDeploymentServiceJSONFile))
    
    StartDeploymentService = json.load(f)
    
    # Raise error if the file is empty
    if not StartDeploymentService:
        logger.error('{} file is empty',format(StartDeploymentServiceJSONFile))
        raise ValueError('{} file is empty'.format(StartDeploymentServiceJSONFile))
    
    # Raise error if the file does not contain the necessary keys
    logger.info('{} file read successfully',format(StartDeploymentServiceJSONFile) )
    if 'StartDeploymentServiceConfigurations' not in StartDeploymentService:
        logger.error('{} file does not contain the necessary keys',format(StartDeploymentServiceJSONFile))
        raise KeyError('{} file does not contain the necessary keys'.format(StartDeploymentServiceJSONFile))
    print(StartDeploymentService)
    DeploymentServiceConfigurations = StartDeploymentService['StartDeploymentServiceConfigurations']

    if 'hostname' not in DeploymentServiceConfigurations or 'port' not in DeploymentServiceConfigurations:
        logger.error('{} file does not contain the necessary keys',format(StartDeploymentServiceJSONFile))
        raise KeyError('{} file does not contain the necessary keys'.format(StartDeploymentServiceJSONFile))
    # Check if hostname or port is empty
    if not DeploymentServiceConfigurations['hostname'] or not DeploymentServiceConfigurations['port']:
        logger.warning('Hostname or Port is empty in {} file',format(StartDeploymentServiceJSONFile))
        # logger.info('Starting DeploymentService with default hostname and port')
        raise ValueError('Hostname or Port is empty in {} file'.format(StartDeploymentServiceJSONFile))
    hostname = DeploymentServiceConfigurations['hostname']
    port = DeploymentServiceConfigurations['port']
    logger.info('Hostname: {} and Port: {} read from {} file'.format(hostname, port,StartDeploymentServiceJSONFile ))
    logger.info('Preparing to start DeploymentService with hostname: {} and port: {}'.format(hostname, port))
    command = [PythonPath, os.path.join(RepositoryDirectory, 'SystemFiles/Executables/DeploymentService.py'), str(hostname), str(port)]
    subprocess.run(command)