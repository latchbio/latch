from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import urljoins


def get_dockerfile_content(image_name: str, wrapper: str):
    url = urljoins(
        "https://raw.githubusercontent.com/snakemake/snakemake-wrappers/", wrapper
    )

    return reindent(
        rf"""
        from {image_name}

        workdir /opt/latch

        run curl -sSL {urljoins(url, "environment.yaml")} --output environment.yaml
        run curl -sSL {urljoins(url, "wrapper.py")} --output wrapper.py

        run mamba env create \
            --file environment.yaml \
            --name workflow

        env PATH=/opt/conda/envs/workflow/bin:$PATH

        workdir /root
        """,
        0,
    )
