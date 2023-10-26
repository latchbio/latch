# latch base image + dependencies for latch SDK --- removing these will break the workflow
from 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:fe0b-main
run pip install latch==2.22.5
run mkdir /opt/latch

# install dependencies for nfcore
run apt-get update && apt-get install -y curl default-jre-headless

# install nfcore
run curl -s https://get.nextflow.io | bash && \
    mv nextflow /usr/local/bin

# copy all code from package (use .dockerignore to skip files)
copy . /root/

# latch internal tagging system + expected root directory --- changing these lines will break the workflow
arg tag
env FLYTE_INTERNAL_IMAGE $tag
workdir /root
