import subprocess
from pathlib import Path
from typing import NamedTuple

from latch import small_task
from latch.types import LatchFile

def ensure_parents_exist(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@small_task
def bwa_map_4(data_genome__fa: LatchFile, data_samples_A__fastq: LatchFile, data_genome__fa__amb: LatchFile, data_genome__fa__ann: LatchFile, data_genome__fa__bwt: LatchFile, data_genome__fa__fai: LatchFile, data_genome__fa__pac: LatchFile, data_genome__fa__sa: LatchFile) -> NamedTuple('bwa_map_4_output', mapped_reads_A__bam=LatchFile):
	Path(data_genome__fa).resolve().rename(ensure_parents_exist(Path("data/genome.fa")))
	Path(data_samples_A__fastq).resolve().rename(ensure_parents_exist(Path("data/samples/A.fastq")))
	Path(data_genome__fa__amb).resolve().rename(ensure_parents_exist(Path("data/genome.fa.amb")))
	Path(data_genome__fa__ann).resolve().rename(ensure_parents_exist(Path("data/genome.fa.ann")))
	Path(data_genome__fa__bwt).resolve().rename(ensure_parents_exist(Path("data/genome.fa.bwt")))
	Path(data_genome__fa__fai).resolve().rename(ensure_parents_exist(Path("data/genome.fa.fai")))
	Path(data_genome__fa__pac).resolve().rename(ensure_parents_exist(Path("data/genome.fa.pac")))
	Path(data_genome__fa__sa).resolve().rename(ensure_parents_exist(Path("data/genome.fa.sa")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "bwa_map:sample=A", "--allowed-rules", "bwa_map", "--local-groupid", "4", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('mapped_reads/A.bam'))

@small_task
def bwa_map_6(data_genome__fa: LatchFile, data_samples_B__fastq: LatchFile, data_genome__fa__amb: LatchFile, data_genome__fa__ann: LatchFile, data_genome__fa__bwt: LatchFile, data_genome__fa__fai: LatchFile, data_genome__fa__pac: LatchFile, data_genome__fa__sa: LatchFile) -> NamedTuple('bwa_map_6_output', mapped_reads_B__bam=LatchFile):
	Path(data_genome__fa).resolve().rename(ensure_parents_exist(Path("data/genome.fa")))
	Path(data_samples_B__fastq).resolve().rename(ensure_parents_exist(Path("data/samples/B.fastq")))
	Path(data_genome__fa__amb).resolve().rename(ensure_parents_exist(Path("data/genome.fa.amb")))
	Path(data_genome__fa__ann).resolve().rename(ensure_parents_exist(Path("data/genome.fa.ann")))
	Path(data_genome__fa__bwt).resolve().rename(ensure_parents_exist(Path("data/genome.fa.bwt")))
	Path(data_genome__fa__fai).resolve().rename(ensure_parents_exist(Path("data/genome.fa.fai")))
	Path(data_genome__fa__pac).resolve().rename(ensure_parents_exist(Path("data/genome.fa.pac")))
	Path(data_genome__fa__sa).resolve().rename(ensure_parents_exist(Path("data/genome.fa.sa")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "bwa_map:sample=B", "--allowed-rules", "bwa_map", "--local-groupid", "6", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('mapped_reads/B.bam'))

@small_task
def samtools_sort_3(mapped_reads_A__bam: LatchFile) -> NamedTuple('samtools_sort_3_output', sorted_reads_A__bam=LatchFile):
	Path(mapped_reads_A__bam).resolve().rename(ensure_parents_exist(Path("mapped_reads/A.bam")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "samtools_sort:sample=A", "--allowed-rules", "samtools_sort", "--local-groupid", "3", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('sorted_reads/A.bam'))

@small_task
def samtools_sort_5(mapped_reads_B__bam: LatchFile) -> NamedTuple('samtools_sort_5_output', sorted_reads_B__bam=LatchFile):
	Path(mapped_reads_B__bam).resolve().rename(ensure_parents_exist(Path("mapped_reads/B.bam")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "samtools_sort:sample=B", "--allowed-rules", "samtools_sort", "--local-groupid", "5", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('sorted_reads/B.bam'))

@small_task
def samtools_index_7(sorted_reads_A__bam: LatchFile) -> NamedTuple('samtools_index_7_output', sorted_reads_A__bam__bai=LatchFile):
	Path(sorted_reads_A__bam).resolve().rename(ensure_parents_exist(Path("sorted_reads/A.bam")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "samtools_index:sample=A", "--allowed-rules", "samtools_index", "--local-groupid", "7", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('sorted_reads/A.bam.bai'))

@small_task
def samtools_index_8(sorted_reads_B__bam: LatchFile) -> NamedTuple('samtools_index_8_output', sorted_reads_B__bam__bai=LatchFile):
	Path(sorted_reads_B__bam).resolve().rename(ensure_parents_exist(Path("sorted_reads/B.bam")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "samtools_index:sample=B", "--allowed-rules", "samtools_index", "--local-groupid", "8", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('sorted_reads/B.bam.bai'))

@small_task
def bcftools_call_2(fa: LatchFile, sorted_reads_A__bam: LatchFile, sorted_reads_B__bam: LatchFile, sorted_reads_A__bam__bai: LatchFile, sorted_reads_B__bam__bai: LatchFile) -> NamedTuple('bcftools_call_2_output', calls_all__vcf=LatchFile):
	Path(fa).resolve().rename(ensure_parents_exist(Path("data/genome.fa")))
	Path(sorted_reads_A__bam).resolve().rename(ensure_parents_exist(Path("sorted_reads/A.bam")))
	Path(sorted_reads_B__bam).resolve().rename(ensure_parents_exist(Path("sorted_reads/B.bam")))
	Path(sorted_reads_A__bam__bai).resolve().rename(ensure_parents_exist(Path("sorted_reads/A.bam.bai")))
	Path(sorted_reads_B__bam__bai).resolve().rename(ensure_parents_exist(Path("sorted_reads/B.bam.bai")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "bcftools_call:", "--allowed-rules", "bcftools_call", "--local-groupid", "2", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('calls/all.vcf'))

@small_task
def plot_quals_1(calls_all__vcf: LatchFile) -> NamedTuple('plot_quals_1_output', plots_quals__svg=LatchFile):
	Path(calls_all__vcf).resolve().rename(ensure_parents_exist(Path("calls/all.vcf")))

	subprocess.run(["snakemake", "-s", "Snakefile", "--target-jobs", "plot_quals:", "--allowed-rules", "plot_quals", "--local-groupid", "1", "--cores", "1", "--force-use-threads"], check=True)
	return (LatchFile('plots/quals.svg', 'latch:///plots/quals.svg'))