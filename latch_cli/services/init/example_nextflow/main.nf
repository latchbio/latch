include { fastqc } from "./workflow.nf"

workflow {
  fastqs = Channel.fromPath("$params.fastqDir/*.fastq*")

  fastqc(fastqs)
}
