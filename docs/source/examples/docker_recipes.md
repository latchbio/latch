# Docker Recipes

## Templates

The Latch SDK  supports creating two new template workflows automatically, namely one already with a Dockerfile that installs R, and one already with a Dockerfile that installs conda. To generate these, simply use latch init with the `--template` option like so:

```shell-session
$ latch init [package_root] --template=[...]
```
Valid values for the template option as of now are `r`, `conda`, and `default`. In particular, the default option (as well as just not providing the flag itself) creates the default `assemble_and_sort` workflow, where the Dockerfile contains instructions for building binaries from source.

---

## Common Dockerfile patterns

### Installing Python Dependencies
**Solution 1**:
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

**Solution 2**:
```Dockerfile
# Install cialign package using pip
RUN pip3 install cialign
```
---
### Installing packages from Anaconda Cloud
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
### Installing R
<br>

```Dockerfile
# Install R version 4.2.1
RUN apt install -y dirmngr apt-transport-https ca-certificates software-properties-common gnupg2
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-key '95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7'
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/debian buster-cran40/'
RUN apt update
RUN apt install -y r-base build-essential
RUN apt-get install libcurl4-openssl-dev

# Example R packages
RUN R -e "install.packages('Rcpp')"
RUN R -e "install.packages('curl')"
RUN R -e "install.packages('RCurl')"
RUN R -e "install.packages('BiocManager')"
```

---
### Build binaries from source
<br>

```Dockerfile
RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\
    unzip bowtie2-2.4.4.zip &&\
    mv bowtie2-2.4.4-linux-x86_64 bowtie2
```