from latch_cli.constants import latch_constants
from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import urljoins


def get_dockerfile_content(wrapper: str):
    url = urljoins(
        "https://raw.githubusercontent.com/snakemake/snakemake-wrappers/", wrapper
    )

    return reindent(
        rf"""
        from {latch_constants.base_image}

        run apt-get update --yes && \
            apt-get install --yes curl vim && \
            curl \
                --location \
                --fail \
                --remote-name \
                https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh && \
            bash Mambaforge-Linux-x86_64.sh -b -p /opt/conda -u && \
            rm Mambaforge-Linux-x86_64.sh

        env PATH=/opt/conda/bin:$PATH

        run curl -sSL {urljoins(url, "environment.yaml")} --output environment.yaml

        run mamba env create \
            --file environment.yaml \
            --name workflow \
            python=3.10

        run mamba install -y -n workflow -c bioconda snakemake snakemake-wrapper-utils

        env PATH=/opt/conda/envs/workflow/bin:$PATH

        run pip install latch

        workdir /root
        arg tag
        env FLYTE_INTERNAL_IMAGE $tag
        """,
        0,
    )
