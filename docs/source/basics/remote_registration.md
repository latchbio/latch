# Remote Registration 

If you do not have access to Docker on your local machine, lack space on your
local filesystem for image layers, or lack fast internet to facilitate timely
registration, you can use the `--remote` flag with `latch register` to build and
upload your workflow's images from a latch-managed, performant, remote machine.


```
$ latch register myworkflow --remote
Initializing registration for /Users/kenny/latch/latch/myworkflow
Connecting to remote server for docker build [alpha]...

```

The registration process will behave as usual but the build/upload will not occur on your local machine.

## Troubleshooting

1. Permission denied

```
Could not open a connection to your authentication agent.
ubuntu@52.38.67.53: Permission denied (publickey).
Unable to register workflow: Unable to establish a connection to remote docker host ssh://ubuntu@52.38.67.53.
```

An SSH agent is used for SSH public key authentication. The error means that an SSH agent may not be running on your computer. You can add an agent with: 

```
eval `ssh-add -s`
ssh-add
```

2. Unable to find a valid key

```
Unable to register workflow: It seems you don't have any (valid) SSH keys set up. Check that any keys that you have are valid, or use a utility like `ssh-keygen` to set one up and try again.
```
Latch uses your SSH key pair for authentication to the remote machine for Docker image build. This error means you either don't have an SSH key pair on your machine or your keys are corrupted. 

**Solution 1**: Verify that you have an SSH key pair by inspecting the `~/.ssh` folder:
```
$ ls ~/.ssh 
config		id_rsa		id_rsa.pub	known_hosts
``` 

If you don't see `id_rsa` or `id_rsa.pub` which represent your private and public key, you have to generate a key pair using `ssh-keygen`:
```
$ ssh-keygen

Generating public/private rsa key pair.
...
```

**Solution 2**: 
Try to use ssh-keygen again to validate the key.
```
ssh-keygen -l -f id_rsa.pub
```
At this point, you should have a response indicating if the key is invalid.


---
*Still can't find a working solution for your issue? Please send an email to hannah@latch.bio, and we will get back to you ASAP!*