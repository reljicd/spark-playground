package com.reljicd

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.reljicd.BedToFasta.{calculateRegionFastaDS, readChromosomeNucleobasesRowDS, readRegionsDS, referenceFilesDirToChromosomeReferencesList}
import org.scalatest.FunSuite


class BedToFastaTest extends FunSuite with DatasetSuiteBase {
  val bedFile = "src/test/data/BedToFastaTest/bed_file.bed"
  val referenceFilesDir = "src/test/data/BedToFastaTest"
  val referenceFastaFile = "src/test/data/BedToFastaTest/reference_fastahack_file.fa"

  test("integration test") {
    import spark.implicits._

    val chromosomeReferencesList = referenceFilesDirToChromosomeReferencesList(referenceFilesDir)

    val chromosomeNucleobasesRowDS = readChromosomeNucleobasesRowDS(spark, chromosomeReferencesList)

    val regionsDS = readRegionsDS(spark, List(bedFile))

    val regionFastaDS = calculateRegionFastaDS(spark, regionsDS, chromosomeNucleobasesRowDS)

    val calculatedFastaDS = regionFastaDS.map(x => x.fasta)

    val referenceFastaDS = spark.read.textFile(referenceFastaFile)
      .map(x => x.toLowerCase)

    assertDatasetEquals(referenceFastaDS, calculatedFastaDS)

  }
}
