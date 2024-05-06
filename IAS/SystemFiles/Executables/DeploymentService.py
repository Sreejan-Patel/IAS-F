from flask import Flask, request, jsonify, redirect, url_for
import os
import zipfile
import tempfile
import sys
from Logging import logger
import subprocess

app = Flask(__name__)




log_message = ""

# Create an html form at the root path
@app.route('/')
def index():
    logger.info('Deployment Service up & running and hostname: {} and port: {}'.format(hostname, port))
    return '''
    <!doctype html>
    <title>Model Deployment Service</title>
    <h1>Model Deployment Service</h1>
    <p>Upload a zip containing 2 files <br> Your model file : model.keras and <br> Your Dependencies file : dependencies.json</p>
    <form method=post enctype=multipart/form-data action=upload>
    <input type="text" id="text-input" name="text-input">
    <input type="submit" value="This is my UserID"><br><br>
      <input type=file name=zipfile>
      <input type=submit value=Upload>
    </form>
    '''


@app.route('/upload', methods=['POST'])
def upload_file():
    # Store the userID entered by the user
    userID = request.form['text-input']
    logger.info('User ID: {} attempts to deploy a model'.format(userID))
    
    # # Create folder for the user if it does not exist
    # if not os.path.exists(os.path.join(os.path.join(os.getcwd(),'SystemFiles'), 'DeployedModels', userID)):
    #     os.makedirs(os.path.join(os.path.join(os.getcwd(),'SystemFiles'), 'DeployedModels', userID))
    
    # if not os.path.exists(os.path.join(os.path.join(os.getcwd(),'SystemFiles'), 'DeployedModels', userID)):
    #     logger.error('User ID: {} folder could not be created'.format(userID))
    # DEPLOYMENT_FOLDER = os.path.join(os.path.join(os.getcwd(),'SystemFiles'), 'DeployedModels', userID)

    # # Count number of folders inside the user's folder
    # count = 0
    # for f in os.listdir(DEPLOYMENT_FOLDER):
    #     if os.path.isdir(os.path.join(DEPLOYMENT_FOLDER, f)):
    #         count += 1

    # # Create new folder titles userID_model_count
    # DEPLOYMENT_FOLDER = os.path.join(DEPLOYMENT_FOLDER, userID + '_model' + str(count))
    DEPLOYMENT_FOLDER = "/home/sreejan/IAS/demo2"
    if not os.path.exists(DEPLOYMENT_FOLDER):
        os.makedirs(DEPLOYMENT_FOLDER)
    else:
        #delete the subtree
        os.system("rm -r "+DEPLOYMENT_FOLDER+"/*")

    if 'zipfile' not in request.files:
        # Print error on webpage
        logger.info('User ID: {} did not select a file'.format(userID))
        log_message = jsonify({'error': 'No file part in the request'}), 400
        return redirect(url_for('index'))
    file = request.files['zipfile']
    if file.filename == '':
        logger.info('User ID: {} did not select a file'.format(userID))
        log_message = jsonify({'error': 'No file selected'}), 400
        return redirect(url_for('index'))
    if file and file.filename.endswith('.zip'):
        try:
            logger.info('User ID: {} uploaded a model. Attempting to save the zip'.format(userID))
            temp_dir = tempfile.mkdtemp()
            temp_path = os.path.join(temp_dir, file.filename)
            file.save(temp_path)


            process_zip_file(temp_path, DEPLOYMENT_FOLDER)


            os.remove(temp_path)
            os.rmdir(temp_dir)

            logger.info('User ID: {} uploaded a model. Model saved successfully'.format(userID))
            log_message = jsonify({'message': 'File successfully uploaded and deployed','userID':userID}), 200
            # Go back to the root path
            print("hello")
            subprocess.run("touch hello", )
            return redirect(url_for('index'))
            # return jsonify({'message': 'File successfully uploaded and deployed','userID':userID}), 200
        except Exception as e:
            logger.error('User ID: {} uploaded a model. Error: {}'.format(userID, str(e)))
            log_message = jsonify({'error': str(e)}), 500
            return redirect(url_for('index'))
            # return jsonify({'error': str(e)}), 500
    else:
        logger.error('User ID: {} uploaded a model. Error: Unsupported file format'.format(userID))
        log_message = jsonify({'error': 'Unsupported file format'}), 400
        return redirect(url_for('index'))

def process_zip_file(zip_path,target_folder):
    """ Unzip the file and deploy its contents to the deployment folder """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:

        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        

        zip_ref.extractall(target_folder)

if __name__ == '__main__':
    logger.info('Attempting to start DeploymentService')
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    logger.info('Hostname: {} and Port: {} read from StartDeploymentService.json file'.format(hostname, port))
    app.run(host=hostname, port=port, debug=False)