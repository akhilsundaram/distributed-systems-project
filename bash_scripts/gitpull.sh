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
pull_latest_code() {
  local vm=$1
  echo "Pulling latest code on $vm"
  ssh $vm "cd $REPO_PATH && git pull"
}

# Main loop to iterate through VMs
for vm in "${VMS[@]}"; do
  pull_latest_code $vm
done

echo "Code update complete on all VMs"