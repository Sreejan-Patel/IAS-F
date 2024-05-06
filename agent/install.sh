#!/bin/bash

# pswd="$1"

# echo "Starting installation on Linux..."

# # Installing Docker
# echo "Installing Docker..."
# sudo -S apt-get update
# sudo -S apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release

# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# sudo -S apt-get update
# sudo -S apt-get install -y docker-ce docker-ce-cli containerd.io

# Adding the current user to the Docker group
# sudo usermod -aG docker $USER

# Installing GNOME Terminal
# echo "Installing GNOME Terminal..."
# # sudo -S apt-get install -y gnome-terminal

# echo "Installation completed successfully!"
sleep 60s
touch testing.txt
echo $1 > testing.txt
