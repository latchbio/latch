from latch_cli.constants import latch_constants
from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import urljoins


def get_dockerfile_content(wrapper: str):
    url = urljoins(
        "https://raw.githubusercontent.com/snakemake/snakemake-wrappers/",
        wrapper,
        "environment.yaml",
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

        workdir /opt/latch

        run curl -sSL https://latch-public.s3.us-west-2.amazonaws.com/pin_python.py --output update_env.py
        run curl -sSL {url} --output environment.yaml

        run python3 -m pip install pyyaml && \
            python3 update_env.py environment.yaml

        run mamba env create \
            --file environment.yaml \
            --name workflow

        env PATH=/opt/conda/envs/workflow/bin:$PATH

        workdir /root

        copy . /root/

        arg tag
        env FLYTE_INTERNAL_IMAGE $tag
        """,
        0,
    )
