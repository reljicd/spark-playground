package com.reljicd

import java.io.File
import java.nio.file.{Files, Paths}

import com.github.tototoshi.csv._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

// monotonicallyIncreasingId field is necessary for sorting output fasta rows as they were in original bed files for the same region
case class Region(chromosome: String, start: Int, end: Int, bedFile: String, monotonicallyIncreasingId: Int)

case class RegionRowsList(region: Region, rowsList: List[Int])

case class RegionRow(region: Region, row: Int)

case class ChromosomeNucleobasesRow(chromosome: String, nucleobases: String, row: Int)

case class RegionNucleobases(region: Region, nucleobases: String)

case class RegionFasta(region: Region, fasta: String)

case class RegionChromosomeFasta(bedFile: String, start: Int, end: Int, chromosome: String, fasta: String)

object BedToFasta {

  val referenceCharsPerRow = 50
  val csvBedFileColumnName = "bed_file_path_selected_sample"
  val chromosomeNames: List[String] = for (i <- "Y" :: "X" :: Range(1, 23).toList) yield "chr" + i

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: BedToFasta <csvFile>  <referenceFiles directory>  <outputFile> ")
      System.exit(1)
    }

    val csvFile = args(0)
    val referenceFilesDir = args(1)
    val outputFile: String = args(2)

    // TODO change local cluster in production
    val sparkSession = SparkSession.builder.master("local").appName("Bed to Fasta").getOrCreate()

    val bedFilesList = csvFileToBedFilesList(csvFile)

    val regionsDS = readRegionsDS(sparkSession, bedFilesList)

    val chromosomeNucleobasesRowDS = readChromosomeNucleobasesRowDS(sparkSession, referenceFilesDir)

    val regionFastaDS = calculateRegionFastaDS(sparkSession, regionsDS, chromosomeNucleobasesRowDS)

    writeFastaToOutputFile(sparkSession, regionFastaDS, outputFile)

    sparkSession.stop()
  }

  /**
    * Read bed files to Dataset of Region case class
    *
    * @param sparkSession
    * @param bedFilesList
    * @return
    */
  def readRegionsDS(sparkSession: SparkSession, bedFilesList: List[String]): Dataset[Region] = {
    import sparkSession.implicits._

    // Map bed file to Region case class
    sparkSession.read.textFile(bedFilesList: _*)
      .withColumn("inputFileName", input_file_name())
      .withColumn("monotonicallyIncreasingId", monotonically_increasing_id())
      .map {
        case Row(value: String, inputFileName: String, monotonicallyIncreasingId: Long) =>
          val bedFileRowWordList = value.split("\t")
          Region(chromosome = bedFileRowWordList(0), start = bedFileRowWordList(1).toInt, end = bedFileRowWordList(2).toInt, bedFile = inputFileName, monotonicallyIncreasingId = monotonicallyIncreasingId.toInt)
      }
  }

  /**
    * Read reference files from referenceFilesDir to Dataset of ChromosomeNucleobasesRow case class
    *
    * @param sparkSession
    * @param referenceFilesDir
    * @return
    */
  def readChromosomeNucleobasesRowDS(sparkSession: SparkSession, referenceFilesDir: String): Dataset[ChromosomeNucleobasesRow] = {
    import sparkSession.implicits._

    // Construct ChromosomeNucleobasesRow dataset from all the .fa files, by iterating through all the extracted chromosomes,
    // and constructing reference files names from .fa files directory name and chromosome names
    var chromosomeNucleobasesRowDS = sparkSession.emptyDataset[ChromosomeNucleobasesRow]
    for (chromosome <- chromosomeNames) {
      val referenceFile = "%s/%s.fa".format(referenceFilesDir, chromosome)
      if (Files.exists(Paths.get(referenceFile)))
        chromosomeNucleobasesRowDS = chromosomeNucleobasesRowDS.union(
          sparkSession.read.textFile(referenceFile)
            // Filter out first line of the format ">chr1"
            .filter(x => !x.contains(">"))
            .withColumn("row", monotonically_increasing_id)
            .map {
              case Row(value: String, row: Long) =>
                ChromosomeNucleobasesRow(chromosome = chromosome, nucleobases = value.toLowerCase(), row = row.toInt)
            })
    }
    chromosomeNucleobasesRowDS
  }

  /**
    * Calculate Dataset of RegionFasta case class, based on regionsDS and chromosomeNucleobasesRowDS
    *
    * @param sparkSession
    * @param regionsDS
    * @param chromosomeNucleobasesRowDS
    * @return
    */
  def calculateRegionFastaDS(sparkSession: SparkSession, regionsDS: Dataset[Region], chromosomeNucleobasesRowDS: Dataset[ChromosomeNucleobasesRow]): Dataset[RegionFasta] = {
    import sparkSession.implicits._

    // Construct dataset of regions and row numbers lists (RegionRowsList), based on referenceCharsPerRow
    val regionsRowsListDS = regionsDS
      .map(region =>
        RegionRowsList(
          region = region,
          rowsList = Math.floor((region.start - 1).toDouble / referenceCharsPerRow).toInt to
            Math.floor(region.end.toDouble / referenceCharsPerRow).toInt toList))

    // Construct dataset of regions and row numbers by flattening all the rows lists
    val regionRowDS = regionsRowsListDS
      .flatMap(RegionRowsList => {
        val regionRowList = scala.collection.mutable.MutableList[RegionRow]()
        RegionRowsList.rowsList.foreach(
          regionRowList += RegionRow(RegionRowsList.region, _)
        )
        regionRowList
      })

    // Join ChromosomeNucleobasesRow dataset and RegionRow dataset on "row" and "chromosome",
    // group them by regions, then reduce for each region by nucleobases concatenation,
    // and then return RegionNucleobase dataset ordered by "region"
    val RegionNucleobasesDS = regionRowDS
      .joinWith(chromosomeNucleobasesRowDS,
        regionRowDS("row") === chromosomeNucleobasesRowDS("row")
          && regionRowDS("region.chromosome") === chromosomeNucleobasesRowDS("chromosome"))
      .map(x => RegionNucleobases(region = x._1.region, nucleobases = x._2.nucleobases))
      .groupByKey(_.region)
      .reduceGroups((x, y) => RegionNucleobases(region = x.region, nucleobases = x.nucleobases + y.nucleobases))
      .map(x => x._2)
      .orderBy("region")

    // Remove nucleobases not in the region, and return RegionFasta dataset
    val regionFastaDS = RegionNucleobasesDS
      .map(x => {
        val stringLength = x.nucleobases.length
        val start = (x.region.start - 1) % referenceCharsPerRow
        val end = stringLength - (referenceCharsPerRow - x.region.end % referenceCharsPerRow)
        RegionFasta(region = x.region, fasta = x.nucleobases.slice(start, end))
      })

    // Reorder rows to original order
    regionFastaDS.orderBy("region.monotonicallyIncreasingId")
  }

  /**
    * Util for transforming csv file to list of bed files paths
    *
    * @param csvFile
    * @return list of bed files paths
    */
  // TODO implement downloading of files from S3
  def csvFileToBedFilesList(csvFile: String): List[String] = {
    val reader = CSVReader.open(new File(csvFile))
    val bedFilesList = reader.allWithHeaders().map(m => m(csvBedFileColumnName))
    bedFilesList
  }

  /**
    * Helper method for writing out fasta files
    *
    * @param sparkSession
    * @param regionFastaDS
    * @param outputFile
    */
  def writeFastaToOutputFile(sparkSession: SparkSession, regionFastaDS: Dataset[RegionFasta], outputFile: String): Unit = {

    import sparkSession.implicits._

    regionFastaDS
      .map(x => RegionChromosomeFasta(x.region.bedFile, x.region.start, x.region.end, x.region.chromosome, x.fasta))
      .coalesce(1) // TODO change to some bigger value in prod
      .write
      .partitionBy("bedFile")
      .option("header", "true")
      .csv(outputFile)
  }

}


