# Metadata

The Snakemake framework was designed to allow developers to both define and execute their workflows. This often means that the workflow parameters are sometimes ill-defined and scattered throughout the project as configuration values, static values in the `Snakefile` or command line flags.

To construct a graphical interface from a snakemake workflow, the file parameters need to be explicitly identified and defined so that they can be presented to scientists to be filled out through a web application.

The `latch_metadata.py` file holds these parameter definitions, along with any styling or cosmetic modifications the developer wishes to make to each parameter.

To generate a `latch_metadata.py` file, type:
```console
latch generate-metadata <path_to_config.yaml>
```

The command automatically parses the existing `config.yaml` file in the Snakemake repository, and create a Python parameters file.

#### Examples

Below is an example `config.yaml` file from the [rna-seq-star-deseq2 workflow](https://github.com/snakemake-workflows/rna-seq-star-deseq2) from Snakemake workflow catalog.

`config.yaml`
```yaml
# path or URL to sample sheet (TSV format, columns: sample, condition, ...)
samples: config/samples.tsv
# path or URL to sequencing unit sheet (TSV format, columns: sample, unit, fq1, fq2)
# Units are technical replicates (e.g. lanes, or resequencing of the same biological
# sample).
units: config/units.tsv


ref:
  # Ensembl species name
  species: homo_sapiens
  # Ensembl release (make sure to take one where snpeff data is available, check 'snpEff databases' output)
  release: 100
  # Genome build
  build: GRCh38

trimming:
  # If you activate trimming by setting this to `True`, you will have to
  # specify the respective cutadapt adapter trimming flag for each unit
  # in the `units.tsv` file's `adapters` column
  activate: False

pca:
  activate: True
  # Per default, a separate PCA plot is generated for each of the
  # `variables_of_interest` and the `batch_effects`, coloring according to
  # that variables groups.
  # If you want PCA plots for further columns in the samples.tsv sheet, you
  # can request them under labels as a list, for example:
  # - relatively_uninteresting_variable_X
  # - possible_batch_effect_Y
  labels: ""

diffexp:
  # variables for whome you are interested in whether they have an effect on
  # expression levels
  variables_of_interest:
    treatment_1:
      # any fold change will be relative to this factor level
      base_level: B
    treatment_2:
      # any fold change will be relative to this factor level
      base_level: C
  # variables whose effect you want to model to separate them from your
  # variables_of_interest
  batch_effects:
    - jointly_handled
  # contrasts for the deseq2 results method to determine fold changes
  contrasts:
    A-vs-B_treatment_1:
      # must be one of the variables_of_interest, for details see:
      # https://www.bioconductor.org/packages/devel/bioc/vignettes/DESeq2/inst/doc/DESeq2.html#contrasts
      variable_of_interest: treatment_1
      # must be a level present in the variable_of_interest that is not the
      # base_level specified above
      level_of_interest: A
  # The default model includes all interactions among variables_of_interest
  # and batch_effects added on. For the example above this implicitly is:
  # model: ~jointly_handled + treatment_1 * treatment_2
  # For the default model to be used, simply specify an empty `model: ""` below.
  # If you want to introduce different assumptions into your model, you can
  # specify a different model to use, for example skipping the interaction:
  # model: ~jointly_handled + treatment_1 + treatment_2
  model: ""


params:
  cutadapt-pe: ""
  cutadapt-se: ""
  star: ""
```

The Python `latch_metadata.py` generated from the Latch command:
```python
from dataclasses import dataclass
import typing

from latch.types.metadata import SnakemakeParameter, SnakemakeFileParameter
from latch.types.file import LatchFile
from latch.types.directory import LatchDir

@dataclass
class ref:
    species: str
    release: int
    build: str


@dataclass
class trimming:
    activate: bool


@dataclass
class pca:
    activate: bool
    labels: str


@dataclass
class treatment_1:
    base_level: str


@dataclass
class treatment_2:
    base_level: str


@dataclass
class variables_of_interest:
    treatment_1: treatment_1
    treatment_2: treatment_2


@dataclass
class A_vs_B_treatment_1:
    variable_of_interest: str
    level_of_interest: str


@dataclass
class contrasts:
    A_vs_B_treatment_1: A_vs_B_treatment_1


@dataclass
class diffexp:
    variables_of_interest: variables_of_interest
    batch_effects: typing.List[str]
    contrasts: contrasts
    model: str


@dataclass
class params:
    cutadapt_pe: str
    cutadapt_se: str
    star: str




# Import these into your `__init__.py` file:
#
# from .parameters import generated_parameters
#
generated_parameters = {
    'samples': SnakemakeFileParameter(
        display_name='samples',
        type=LatchFile,
        config=True,
    ),
    'units': SnakemakeFileParameter(
        display_name='units',
        type=LatchFile,
        config=True,
    ),
    'ref': SnakemakeParameter(
        display_name='ref',
        type=ref,
        default=ref(species='homo_sapiens', release=100, build='GRCh38'),
    ),
    'trimming': SnakemakeParameter(
        display_name='trimming',
        type=trimming,
        default=trimming(activate=False),
    ),
    'pca': SnakemakeParameter(
        display_name='pca',
        type=pca,
        default=pca(activate=True, labels=''),
    ),
    'diffexp': SnakemakeParameter(
        display_name='diffexp',
        type=diffexp,
        default=diffexp(variables_of_interest=variables_of_interest(treatment_1=treatment_1(base_level='B'), treatment_2=treatment_2(base_level='C')), batch_effects=['jointly_handled'], contrasts=contrasts(A_vs_B_treatment_1=A_vs_B_treatment_1(variable_of_interest='treatment_1', level_of_interest='A')), model=''),
    ),
    'params': SnakemakeParameter(
        display_name='params',
        type=params,
        default=params(cutadapt_pe='', cutadapt_se='', star=''),
    ),
}
```

Once the workflow is registered to Latch, it will receive an interface like below:

![Snakemake workflow GUI](../assets/snakemake/metadata.png)
