# Troubleshooting Guide

This guide goes through the frequently encoutered errors and how to resolve them. 

## Docker is running on Windows but I still can't register my workflow.
You'd have to use a virtualized Linux shell on Windows. 

* Follow the instructions [here](https://docs.microsoft.com/en-us/windows/wsl/install) to install WSL2. 
* If you are using Docker Desktop, follow the steps [here](https://docs.docker.com/desktop/windows/wsl/) to enable Docker support in WSL2 2 distros.
* Enter the Linux shell by typing `wsl` before running any Latch commands. 

## Task with different structure already exists error.
Simply bump your version in the `version` file and re-register.