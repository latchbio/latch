process gunzipProcess {
  input:
    path compressed
  output:
    path decompressed

  script:
    decompressed = compressed.toString() - ".gz"
    """
    gunzip -c $compressed > $decompressed
    """
}


process fastqcProcess {
  input:
    path x
  output:
    path "*_fastqc.html", emit: report

  script:
    """
    fastqc $x
    """
}


workflow fastqc {
  take:
    fastqs // path(*.fastq)

  main:
    sorted = fastqs.branch({
      compressed: it.toString().endsWith('.gz')
      uncompressed: true
    })

    if (params.decompress_files) {
      gunzipProcess(sorted.compressed)

      fastqs = sorted.uncompressed.mix(gunzipProcess.out)
    } else {
      fastqs = sorted.uncompressed.mix(sorted.compressed)
    }

    fastqcProcess(fastqs)

  emit:
    report = fastqcProcess.out.report
}
