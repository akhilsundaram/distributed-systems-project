#!/bin/bash

# List of VM hostnames or IP addresses
VMS=(
  "fa24-cs425-5901.cs.illinois.edu"
  "fa24-cs425-5902.cs.illinois.edu"
  "fa24-cs425-5903.cs.illinois.edu"
  "fa24-cs425-5904.cs.illinois.edu"
  "fa24-cs425-5905.cs.illinois.edu"
  "fa24-cs425-5906.cs.illinois.edu"
  "fa24-cs425-5907.cs.illinois.edu"
  "fa24-cs425-5908.cs.illinois.edu"
  "fa24-cs425-5909.cs.illinois.edu"
  "fa24-cs425-5910.cs.illinois.edu"
)

# Git repository path on each fa24-cs425-590
REPO_PATH="/home/code/g59"

# Function to pull latest code on a VM
# SSH user
SSH_USER="anuragc3"

# Sudo password (WARNING: Storing passwords in scripts is not secure)
SUDO_PASSWORD="H1gh3rSt!dy2$"

# Function to pull latest code on a VM as root
pull_latest_code() {
  local vm=$1
  echo "Pulling latest code on $vm as root"
  ssh -t $SSH_USER@$vm  << EOF
    expect -c "
      spawn sudo su -
      expect \"password for $SSH_USER:\"
      send \"$SUDO_PASSWORD\r\"
      expect \"#\"
      send \"cd $REPO_PATH && git pull\r\"
      expect \"#\"
      send \"exit\r\"
      expect eof
    "
EOF
}
# Main loop to iterate through VMs
for vm in "${VMS[@]}"; do
  pull_latest_code $vm
done

