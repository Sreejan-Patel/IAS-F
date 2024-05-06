import os
import logbook
from Repository import RepositoryDirectory

LOGS_FOLDER = os.path.join(os.path.join(RepositoryDirectory, 'SystemFiles'), 'Logs')
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Create a log file for the DeploymentService
DeploymentServiceLog = logbook.Logger('DeploymentService')
logbook.FileHandler(os.path.join(LOGS_FOLDER, 'DeploymentService.log')).push_application()

logger = logbook.Logger('DeploymentService')