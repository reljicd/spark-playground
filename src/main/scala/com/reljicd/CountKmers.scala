package com.reljicd

import org.apache.spark.sql.SparkSession

object CountKmers {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: CountKmers <hgfasta>  <outputfile>  <filterabove> ")
      System.exit(1)
    }

    val input = args(0)
    val K = args(1).toInt
    val N = args(2).toInt
    val processLessThan150 = args(3).toInt

    val sparkSession = SparkSession.builder.master("local").appName("Count Kmers").getOrCreate()

    val sc = sparkSession.sparkContext
    val broadcastK = sc.broadcast(K)
    val broadcastN = sc.broadcast(N)

    val records = sc.textFile(input)

    // remove the records, which are not an actual sequence data
    val filteredRDD = records.filter(line => {
      !(
        line.startsWith("@") ||
          line.startsWith("+") ||
          line.startsWith(";") ||
          line.startsWith("!") ||
          line.startsWith("~") ||
          line.trim().startsWith(">"))
    })
    var short_fragsOnly = filteredRDD
    if (processLessThan150 == 1) {
      short_fragsOnly = filteredRDD.filter(line => {
        line.length() <= 150
      })
    }

    val toppercaseRDD = short_fragsOnly.map(line => {
      line.toUpperCase()
    })

    val kmers = toppercaseRDD.flatMap(_.sliding(broadcastK.value, 1).map((_, 1)))

    // find frequencies of kmers
    val kmersGrouped = kmers.reduceByKey(_ + _).filter(kv => kv._2 > broadcastN.value)

    println(kmersGrouped.count())

  }

}
