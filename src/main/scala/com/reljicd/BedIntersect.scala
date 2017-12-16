package com.reljicd

import org.apache.spark.{SparkConf, SparkContext}


object BedIntersect {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Profile Count Application").setMaster("local")
    val sc = new SparkContext(conf)
    //    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    println("Oh, hi Mark!")
    if (args.length < 4) {
      System.err.println("Usage: BedIntersect <bedFile1Path>  <bedFile2Path> <saveToPath>  <gridSize>")
      System.err.println("Usage:  <gridSize> is optional parameter. Default value is 100 ")
      System.exit(1)
    }
    println("Oh, hi Mark!")
    val bedFile1 = args(0)
    val bedFile2 = args(1)
    val saveToPath = args(2)
    val gridSize = args(3).toInt

    val bedFileData1 = sc.textFile(bedFile1)
    val bedFileDataMap1 = bedFileData1.map(_.split("\t")).map(tss => (tss(0), tss(1).toInt, tss(2).toInt, tss(3), tss(4).toInt, tss(5)))

    val bedFileData2 = sc.textFile(bedFile2)
    val bedFileDataMap2 = bedFileData2.map(_.split("\t")).map(tss => (tss(0), tss(1).toInt, tss(2).toInt, tss(3), tss(4).toInt, tss(5)))


    val b1GridPreProcessing = bedFileDataMap1.map(x =>
      ((x._1, Range(x._2 / gridSize + 1, x._3 / gridSize + 2)),
        x))

    val b1Grid = b1GridPreProcessing.flatMap(x => {
      var ml = scala.collection.mutable.MutableList[((String, Int), (String, Int, Int, String, Int, String))]()
      x._1._2.foreach(num => {
        ml += (((x._1._1, num), x._2))
      })
      ml
    })

    val b2Grid = bedFileDataMap2.map(x => ((x._1,
      Range(x._2 / gridSize + 1, x._3 / gridSize + 2)),
      (x._1, x._2, x._3, x._4, x._5, x._6))).
      flatMap(x => {
        var ml = scala.collection.mutable.MutableList[((String, Int), (String, Int, Int, String, Int, String))]()
        x._1._2.foreach(num => {
          ml += (((x._1._1, num), x._2))
        })
        ml
      })

    val result1 = b1Grid.join(b2Grid)

    val result2 = result1.filter(e => intersects(e._2._1._2, e._2._1._3, e._2._2._2, e._2._2._3))

    val result9 = result2.map(f => f._2._1).distinct().sortBy(f => (f._1, f._2), ascending = true, 60)
    result9.saveAsTextFile(saveToPath)
    println("Oh, hi Mark!")
    sc.stop()
    //    spark.stop()
  }

  def intersects(a1: Long, a2: Long, b1: Long, b2: Long): Boolean = {
    math.max(a1, b1) <= math.min(a2, b2)
  }
}
