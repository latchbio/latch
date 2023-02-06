# Latch preamble (do not change)
FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:ace9-main

RUN apt-get update -y && \
    apt-get install -y curl unzip
# end preamble

# Install any apt packages
# RUN apt-get install -y package_1 package_2

# Install any python packages
# COPY requirements.txt /root/requirements.txt
# RUN python3 -m pip install -r /root/requirements.txt

# Set any environment variables
# ENV MY_ENV="foo"

# Latch epilogue (do not change)
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
# end epilogue
