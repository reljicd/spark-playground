package com.reljicd

import java.io.File

import com.github.tototoshi.csv._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

case class Region(chromosome: String, start: Int, end: Int)

case class RegionRowsList(region: Region, rowsList: List[Int])

case class RegionRow(region: Region, row: Int)

case class ChromosomeNucleobasesRow(chromosome: String, nucleobases: String, row: Int)

case class RegionNucleobases(region: Region, nucleobases: String)

case class RegionFasta(region: Region, fasta: String)

object BedToFasta {

  val referenceCharsPerRow = 50
  val csvBedFileColumnName = "bed_file_path_selected_sample"

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: BedToFasta <csvFile>  <referenceFiles directory>  <outputFile> ")
      System.exit(1)
    }

    val csvFile = args(0)
    val referenceFilesDir = args(1)
    val outputFile = args(2)

    val sparkSession = SparkSession.builder.master("local").appName("Bed to Fasta").getOrCreate()

    bedToFasta(sparkSession, csvFile, referenceFilesDir, outputFile)

    sparkSession.stop()
  }

  /**
    *
    * @param sparkSession
    * @param csvFile
    * @param referenceFilesDir
    * @param outputFile
    * @return Dataset of RegionFasta
    */
  def bedToFasta(sparkSession: SparkSession, csvFile: String, referenceFilesDir: String, outputFile: String): Dataset[RegionFasta] = {
    val bedFilesList = csvFileToBedFilesList(csvFile)
    bedToFasta(sparkSession, bedFilesList, referenceFilesDir, outputFile)
  }

  def bedToFasta(sparkSession: SparkSession, bedFilesList: List[String], referenceFilesDir: String, outputFile: String): Dataset[RegionFasta] = {
    import sparkSession.implicits._

    // Map bed file to Region case class
    var regionsDataSet = sparkSession.emptyDataset[Region]
    for (bedFile <- bedFilesList) {
      regionsDataSet = regionsDataSet.union(
        sparkSession.read.textFile(bedFile)
          .map(_.split("\t"))
          .map(x => Region(chromosome = x(0), start = x(1).toInt, end = x(2).toInt)))
    }

    // Extract chromosomes from regionsDataSet
    val chromosomes = regionsDataSet.map(x => x.chromosome).distinct().collect()


    // Construct ChromosomeNucleobasesRow dataset from all the .fa files, by iterating through all the extracted chromosomes,
    // and constructing reference files names from .fa files directory name and chromosome names
    var chromosomeNucleobasesRowDataSet = sparkSession.emptyDataset[ChromosomeNucleobasesRow]
    for (chromosome <- chromosomes) {
      val referenceFile = "%s/%s.fa".format(referenceFilesDir, chromosome)
      chromosomeNucleobasesRowDataSet = chromosomeNucleobasesRowDataSet.union(
        sparkSession.read.textFile(referenceFile)
          .withColumn("row", monotonically_increasing_id)
          .map(x => ChromosomeNucleobasesRow(chromosome = chromosome, nucleobases = x.getString(0), row = x.getLong(1).toInt)))
    }

    // Construct dataset of regions and row numbers lists (RegionRowsList), based on referenceCharsPerRow
    val regionsRowsListDataSet = regionsDataSet
      .map(region =>
        RegionRowsList(
          region = region,
          rowsList = Math.floor(region.start.toDouble / referenceCharsPerRow).toInt to
            Math.ceil(region.end.toDouble / referenceCharsPerRow).toInt toList))

    // Construct dataset of regions and row numbers by flattening all the rows lists
    val regionRowDataSet = regionsRowsListDataSet
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
    val RegionNucleobasesDataSet = regionRowDataSet
      .joinWith(chromosomeNucleobasesRowDataSet,
        regionRowDataSet("row") === chromosomeNucleobasesRowDataSet("row")
          && regionRowDataSet("region.chromosome") === chromosomeNucleobasesRowDataSet("chromosome"))
      .map(x => RegionNucleobases(region = x._1.region, nucleobases = x._2.nucleobases))
      .groupByKey(_.region)
      .reduceGroups((x, y) => RegionNucleobases(region = x.region, nucleobases = x.nucleobases + y.nucleobases))
      .map(x => x._2)
      .orderBy("region")

    // Remove nucleobases not in the region, and return RegionFasta dataset
    val regionFastaDataSet = RegionNucleobasesDataSet
      .map(x => {
        val stringLength = x.nucleobases.length
        val start = x.region.start % referenceCharsPerRow
        val end = stringLength - (referenceCharsPerRow - x.region.end % referenceCharsPerRow)
        RegionFasta(region = x.region, fasta = x.nucleobases.slice(start, end))
      }
      )

    regionFastaDataSet
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

}