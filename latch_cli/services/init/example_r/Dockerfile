FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

# Install R
RUN apt-get update
RUN apt-get install -y software-properties-common &&\
    add-apt-repository "deb http://cloud.r-project.org/bin/linux/debian buster-cran40/" &&\
    apt-get install -y r-base r-base-dev libxml2-dev libcurl4-openssl-dev libssl-dev wget

# Import packages necessary for your workflow
COPY imports.R /root/imports.R
RUN chmod +x /root/imports.R && /root/imports.R

# You can add other R files here by copying them over,
# and you can then run them within your tasks. Don't forget shebangs!

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
