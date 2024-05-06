#!/bin/bash

# Assigning arguments to variables
PASSWORD=changeme
SERVER_IP=196.168.100.5
SERVER_PATH=/home/sreejan/NFS
MOUNT_POINT="./mount"

# Get sudo privileges
echo "$PASSWORD" | sudo -Sv

# Installing nfs-common if it is not already installed
if ! dpkg -l | grep -qw nfs-common; then
    echo "nfs-common is not installed. Installing..."
    echo "$PASSWORD" | sudo -S apt update
    echo "$PASSWORD" | sudo -S apt install nfs-common -y
fi

# Creating the local mount directory if it does not exist
if [ ! -d "$MOUNT_POINT" ]; then
    echo "Creating local mount directory at $MOUNT_POINT..."
    echo "$PASSWORD" | sudo -S mkdir -p "$MOUNT_POINT"
fi

# Mounting the NFS directory
echo "Mounting NFS directory from $SERVER_IP:$SERVER_PATH to $MOUNT_POINT..."
echo "$PASSWORD" | sudo -S mount "$SERVER_IP":"$SERVER_PATH" "$MOUNT_POINT"

echo "NFS directory has been mounted to $MOUNT_POINT."




