# Docker Recipes

This page contains some common implementation patterns used in Dockerfiles when building images for workflows.

## Installing Python Dependencies
### Solution 1:
* Create a `requirements.txt` document in the same directory as your Dockerfile.
* Specify the Python packages and their versions like so.
```
biopython==1.79
numpy==1.18
tqdm==4.45
cython
clipkit==1.3.0
```
* In your Dockerfile, add:
```Dockerfile
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
```

### Solution 2: 
```Dockerfile
# Install cialign package using pip
RUN pip3 install cialign
```
---
## Installing packages from Anaconda Cloud
<br>

```Dockerfile
# Get miniconda
RUN curl -O \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

# linux dependencies
RUN conda install -c defaults -c conda-forge -c bioconda -y -n base --debug -c bioconda trimmomatic flash numpy cython jinja2 tbb=2020.2 \
  && conda clean --all --yes

```

---
## Build binaries from source
<br>

```Dockerfile
RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\
    unzip bowtie2-2.4.4.zip &&\
    mv bowtie2-2.4.4-linux-x86_64 bowtie2
```