#!/bin/bash

# Assigning arguments to variables
PASSWORD=changeme
SUBNET=196.168.100.5/24
EXPORT_PATH=/home/sreejan/NFS

# Get sudo privileges
echo "$PASSWORD" | sudo -Sv

# Installing nfs-kernel-server if it is not already installed
if ! dpkg -l | grep -qw nfs-kernel-server; then
    echo "nfs-kernel-server is not installed. Installing..."
    echo "$PASSWORD" | sudo -S apt update
    echo "$PASSWORD" | sudo -S apt install nfs-kernel-server -y
fi

# Creating the export directory if it does not exist
if [ ! -d "$EXPORT_PATH" ]; then
    echo "The directory $EXPORT_PATH does not exist. Creating..."
    echo "$PASSWORD" | sudo -S mkdir -p "$EXPORT_PATH"
fi

# Setting up NFS export
echo "Exporting $EXPORT_PATH to the subnet $SUBNET with read-write access..."
echo "$PASSWORD" | sudo -S exportfs "$SUBNET":"$EXPORT_PATH" -o rw

echo "NFS export setup completed."
