package com.reljicd

import java.io.File
import java.nio.file.{Files, Paths}

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.github.tototoshi.csv._
import com.reljicd.utils.S3Utils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class Sample(uuid: String, bedFileLocalPath: String)

case class ChromosomeReference(chromosome: String, referenceFileLocalPath: String)

// monotonicallyIncreasingId field is necessary for sorting output fasta rows as they were in original bed files for the same region
case class Region(chromosome: String, start: Int, end: Int, bedFile: String, monotonicallyIncreasingId: Int)

case class RegionRowsList(region: Region, rowsList: List[Int])

case class RegionRow(region: Region, row: Int)

case class ChromosomeNucleobasesRow(chromosome: String, nucleobases: String, row: Int)

case class RegionNucleobases(region: Region, nucleobases: String)

case class RegionFasta(region: Region, fasta: String)

case class UUIDFasta(uuid: String, fasta: String)

object BedToFasta extends S3Utils {

  val referenceCharsPerRow = 50
  val csvBedFileColumnName = "bed_file_path_selected_sample"
  val defaultCSVOutputDir = "temp/s3/csv"
  val defaultOutputDir = "temp/output/bed_to_fa"
  val defaultReferenceFilesDir = "temp/s3/referenceFiles"
  val defaultBedFilesDir = "temp/s3/bed"

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: BedToFasta <csvFile>  <referenceFilesDir>  <outputDir>  <samplesFilter>")
      System.exit(1)
    }

    val csvFile: String = args(0)
    val referenceFilesDir: String = args(1)
    val outputDir: String = if (args(2).startsWith("s3")) defaultOutputDir else args(2)
    val samplesFilter: List[String] = if (args.length > 3) args(3).split(",").toList else List.empty[String]

    val sparkSession = SparkSession.builder
      .master("local") // TODO change local cluster in production
      .appName("Bed to Fasta")
      .getOrCreate()

    val samplesList = csvFileToSamplesList(csvFile, samplesFilter)

    val bedFilesPathsList = samplesList.map(x => x.bedFileLocalPath)

    val regionsDS = readRegionsDS(sparkSession, bedFilesPathsList)

    val chromosomeReferencesList = referenceFilesDirToChromosomeReferencesList(referenceFilesDir)

    val chromosomeNucleobasesRowDS = readChromosomeNucleobasesRowDS(sparkSession, chromosomeReferencesList)

    val regionFastaDS = calculateRegionFastaDS(sparkSession, regionsDS, chromosomeNucleobasesRowDS)

    writeFastaToOutputDir(sparkSession, regionFastaDS, samplesList, outputDir)

    sparkSession.stop()

    // Write to S3 if <outputDir> is S3 location
    if (args(2).startsWith("s3")) writeFaFilesToS3(args(2), outputDir, samplesList)
  }

  /**
    * Read bed files to Dataset of Region case class
    *
    * @param sparkSession
    * @param bedFilesPathsList
    * @return
    */
  def readRegionsDS(sparkSession: SparkSession, bedFilesPathsList: List[String]): Dataset[Region] = {
    import sparkSession.implicits._

    // Map bed file to Region case class
    sparkSession.read.textFile(bedFilesPathsList: _*)
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
    * @param chromosomeReferencesList
    * @return
    */
  def readChromosomeNucleobasesRowDS(sparkSession: SparkSession, chromosomeReferencesList: List[ChromosomeReference]): Dataset[ChromosomeNucleobasesRow] = {
    import sparkSession.implicits._

    // Construct ChromosomeNucleobasesRow dataset from all the .fa files, by iterating through all the extracted chromosomes,
    // and constructing reference files names from .fa files directory name and chromosome names
    var chromosomeNucleobasesRowDS = sparkSession.emptyDataset[ChromosomeNucleobasesRow]
    for (chromosomeReference <- chromosomeReferencesList) {
      chromosomeNucleobasesRowDS = chromosomeNucleobasesRowDS.union(
        sparkSession.read.textFile(chromosomeReference.referenceFileLocalPath)
          // Filter out first line of the format ">chr1"
          .filter(x => !x.contains(">"))
          .withColumn("row", monotonically_increasing_id)
          .map {
            case Row(value: String, row: Long) =>
              ChromosomeNucleobasesRow(chromosome = chromosomeReference.chromosome, nucleobases = value.toLowerCase(), row = row.toInt)
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
    * Util for transforming csv file to list of Samples
    * Reads CSV file either from S3 or from local path, and than parse it into Samples
    *
    * @param csvFilePath
    * @param samplesFilter
    * @return list of bed files paths
    */
  def csvFileToSamplesList(csvFilePath: String, samplesFilter: List[String]): List[Sample] = {
    val csvFile: File = if (csvFilePath.startsWith("s3")) {
      S3FileToLocalFile(csvFilePath, defaultCSVOutputDir)
    } else new File(csvFilePath)

    val reader: CSVReader = CSVReader.open(csvFile)

    reader.allWithHeaders()
      .map(row => Sample(uuid = row("sample_uuid"), bedFileLocalPath = row(csvBedFileColumnName)))
      // Filter samples by samplesFilter if it is non empty
      .filter(sample => if (samplesFilter.nonEmpty) samplesFilter.contains(sample.uuid) else true)
      // Now, if bedFileLocalPath points to S3, download all files from S3 and reassign bedFileLocalPath to local file path
      .map(sample => {
      val bedFileLocalPath = if (sample.bedFileLocalPath.startsWith("s3")) {
        S3FileToLocalFile(sample.bedFileLocalPath, defaultBedFilesDir).getAbsolutePath
      } else sample.bedFileLocalPath
      Sample(uuid = sample.uuid, bedFileLocalPath = bedFileLocalPath)
    })
  }

  /**
    * Util for transforming reference files location to list of ChromosomeReferences
    * Check for the existence of reference files, if they are on S3 downloads them locally, and
    * returns list ChromosomeReferences
    *
    * @param referenceFilesDir
    * @return list of local reference files paths
    */
  def referenceFilesDirToChromosomeReferencesList(referenceFilesDir: String): List[ChromosomeReference] = {

    val chromosomeNames: List[String] = for (i <- "Y" :: "X" :: Range(1, 23).toList) yield "chr" + i

    chromosomeNames
      .map(chromosome => {
        var referenceFilePath = s"$referenceFilesDir/$chromosome.fa"
        if (referenceFilePath.startsWith("s3")) {
          try {
            referenceFilePath = S3FileToLocalFile(referenceFilePath, defaultReferenceFilesDir).getAbsolutePath
          } catch {
            case ex: AmazonS3Exception =>
              println(ex)
          }
        }
        ChromosomeReference(chromosome = chromosome, referenceFileLocalPath = referenceFilePath)
      })
      .filter(chromosomeReference => Files.exists(Paths.get(chromosomeReference.referenceFileLocalPath)))

  }

  /**
    * Helper method for writing out fasta files
    *
    * @param sparkSession
    * @param regionFastaDS
    * @param samplesList
    * @param outputDir
    */
  def writeFastaToOutputDir(sparkSession: SparkSession, regionFastaDS: Dataset[RegionFasta], samplesList: List[Sample], outputDir: String): Unit = {

    import sparkSession.implicits._

    def getUUIDForBedFile(bedFile: String): String = {
      samplesList.filter(x => bedFile.endsWith(x.bedFileLocalPath)).head.uuid
    }

    regionFastaDS
      .map(x => UUIDFasta(uuid = getUUIDForBedFile(x.region.bedFile), fasta = x.fasta))
      .coalesce(1) // TODO change to some bigger value in prod
      .write
      .partitionBy("uuid")
      .csv(outputDir)
  }

  def writeFaFilesToS3(s3OutputLocation: String, localOutputLocation: String, samplesList: List[Sample]): Unit = {
    for (sample <- samplesList) {
      val sampleOutputDirectory: File = new File(s"$localOutputLocation/uuid=${sample.uuid}")
      val sampleFile: File = FileUtils.listFiles(sampleOutputDirectory, new WildcardFileFilter("*.csv"), null).iterator().next()
      val s3FilePath = s"$s3OutputLocation/${sample.uuid}.fa"
      uploadFileToS3(sampleFile, s3FilePath)
    }
  }

}


