package com.reljicd

import java.io.File

import com.reljicd.utils.S3Utils
import org.apache.spark.sql.SparkSession

object CountKmers extends S3Utils {

  val defaultOutputDir = "temp/output/kmer_counts"
  val defaultFaFilesDir = "temp/s3/fa"

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: MultiFileKmerCount <inputDir> <samplesUUIDs> <outputDir> <kmerLength> <filteringCount> <processLessThan150>")
      System.exit(1)
    }

    val inputDir: String = args(0)
    val samplesUUIDs: List[String] = args(1).split(",").toList
    val outputDir: String = if (args(2).startsWith("s3")) defaultOutputDir else args(2)
    val K: Int = args(3).toInt
    val N: Int = args(4).toInt
    val processLessThan150: Boolean = if (args(5).toInt == 0) false else true

    val sparkSession = SparkSession.builder
      .master("local") // TODO change local cluster in production
      .appName("Count Kmers")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val broadcastK = sc.broadcast(K)
    val broadcastN = sc.broadcast(N)

    // If files are on S3, download them first
    val faFilesPaths: List[String] = samplesUUIDs.map(sampleUUID => {
      if (inputDir.startsWith("s3")) {
        S3FileToLocalFile(s"$inputDir/$sampleUUID.fa", defaultFaFilesDir).getAbsolutePath
      } else s"$inputDir/$sampleUUID.fa"
    })

    faFilesPaths.foreach { faFilePath =>

      val records = sc.textFile(faFilePath, 1000)

      // remove the records, which are not an actual sequence data
      val filteredRDD = records.filter(line => {
        !(line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";") ||
          line.startsWith("!") ||
          line.startsWith("~") ||
          line.trim().startsWith(">"))
      })

      var short_fragsOnly = filteredRDD
      if (processLessThan150) {
        short_fragsOnly = filteredRDD.filter(line => {
          line.length() <= 150
        })
      }

      val toUppercaseRDD = short_fragsOnly.map(line => line.toUpperCase())

      val kmers = toUppercaseRDD.flatMap(_.sliding(broadcastK.value, 1).map((_, 1)))

      // find frequencies of kmers
      val kmersGrouped = kmers.reduceByKey(_ + _).filter(kv => kv._2 > broadcastN.value)

      val partitions = kmersGrouped.mapPartitions(_.toList.sortBy(_._1).iterator)
      partitions.map(kv => kv._1 + ", " + kv._2.toLong)
        .coalesce(1)
        .saveAsTextFile(s"$outputDir/${getFileNameFromS3Url(faFilePath)}")
    }

    sparkSession.stop()

    // Write to S3 if <outputDir> is S3 location
    if (args(2).startsWith("s3")) uploadDirToS3(new File(outputDir), args(2))

  }
}



