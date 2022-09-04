FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

# The series of commands below installs 'Miniconda', which provides conda
ENV CONDA_DIR /opt/conda
ENV PATH=$CONDA_DIR/bin:$PATH

RUN apt-get update
RUN apt-get install -y curl \
    && curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && conda init bash

# The line below creates your environment and installs whatever packages you want.
# Fill up 'requirements.txt' with the names of the packages you want to install.
# You can add channels to the command by using '-c [channel_name]' BEFORE --file.
COPY requirements.txt /root/requirements.txt
RUN conda create -y -n your_env -c conda-forge --file /root/requirements.txt

# Set shell
SHELL ["conda", "run", "-n", "your_env", "/bin/bash", "-c"]

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN /opt/conda/bin/pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
