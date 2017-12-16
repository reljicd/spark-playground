package com.reljicd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Region(chromosome: String, start: Int, end: Int)

case class RegionRows(region: Region, rowsRange: List[Int])

case class RegionRow(region: Region, row: BigInt)

case class ReferenceFileRow(value: String, row: BigInt)

case class RegionReferenceRow(region: Region, value: String, row: BigInt)

case class RegionReferenceRowValue(region: Region, value: String)

case class RegionFasta(region: Region, value: String)

object BedToFasta {

  val referenceCharsPerRow = 50

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: BedToFasta <bedFile>  <referenceFile>  <outputFile> ")
      System.exit(1)
    }

    val bedFile = args(0)
    val referenceFile = args(1)
    val outputFile = args(2)

    val sparkSession = SparkSession.builder.master("local").appName("Bed to Fasta").getOrCreate()
    //    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    //    val bedFileData = sc.textFile(bedFile)
    //    val bedFileDataMap = bedFileData
    //      .map(_.split("\t"))
    //      .map(x => (x(0), x(1).toInt, x(2).toInt, x(3), x(4).toInt, x(5)))
    val referenceFileDataSet = sparkSession.read.textFile(referenceFile)
      .withColumn("row", monotonically_increasing_id)
      .as[ReferenceFileRow]
    //      .map(_.toList)
    //      .flatMap(identity)

    //    val regions = bedFileData.map(_.split("\t")).map(x => (x(0), x(1).toInt, x(2).toInt))


    //    val bedFileDataMap1 = bedFileDataMap.map {
    //      x => Bed(x(0), x(1).toInt, x(2).toInt, x(5))
    //    }

    //    val bedFileDataSet = bedFileDataMap1.toDF().as[Bed]

    //        val bedFileDataset = sparkSession.read.format("tsv")
    //          .option("sep", "\t")
    //          .option("inferSchema", "true")
    //          .textFile(bedFile).as[Bed]

    val regionsDataSet = sparkSession.read.textFile(bedFile)
      .map(_.split("\t"))
      .map(x => Region(chromosome = x(0), start = x(1).toInt, end = x(2).toInt))
    //    val bedFileDataSet2 = sparkSession.read.textFile(bedFile).map(_.split("\t")).toDF("chromosome", "start", "end", "", "", "strand").as[Bed]

    val regionsRowsDataSet = regionsDataSet
      .map(region =>
        RegionRows(
          region = region,
          rowsRange = Math.floor(region.start.toDouble / referenceCharsPerRow).toInt to
            Math.ceil(region.end.toDouble / referenceCharsPerRow).toInt toList))
    //          rowStart = Math.floor(region.start.toDouble / referenceCharsPerRow).toInt,
    //          rowEnd = Math.ceil(region.end.toDouble / referenceCharsPerRow).toInt))

    //    sc.stop()

    val regionRowDataSet = regionsRowsDataSet
      .flatMap(regionRows => {
        val regionRowList = scala.collection.mutable.MutableList[RegionRow]()
        regionRows.rowsRange.foreach(
          regionRowList += RegionRow(regionRows.region, _)
        )
        regionRowList
      })

    val regionReferenceRowDataSet = regionRowDataSet
      .joinWith(referenceFileDataSet, regionRowDataSet("row") === referenceFileDataSet("row"))
      //      .as[RegionRowReferenceFileRow]
      .map(x => RegionReferenceRow(region = x._1.region, value = x._2.value, row = x._2.row))
    //        .groupByKey(x => x._1)
    //      .reduceGroups((x, y) => (x._1, x._2 + y._2))

    val regionReferenceRowsListDataSet = regionReferenceRowDataSet.groupByKey(_.region)
      .reduceGroups((x, y) => RegionReferenceRow(region = x.region, value = x.value + y.value, row = 0))
      .map(x => RegionReferenceRowValue(region = x._1, value = x._2.value))
      .orderBy("region")

    val regionFastaDataSet = regionReferenceRowsListDataSet
      .map(x => {
        val stringLength = x.value.length
        val start = x.region.start%referenceCharsPerRow
        val end = stringLength - (referenceCharsPerRow - x.region.end%referenceCharsPerRow)
        RegionFasta(region = x.region, value = x.value.slice(start, end))
      }
      )

    sparkSession.stop()
  }
}
