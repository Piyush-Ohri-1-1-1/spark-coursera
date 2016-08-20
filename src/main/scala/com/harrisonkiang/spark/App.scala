package com.harrisonkiang.spark

/**
 * A self-contained Spark application
 *
 */

import java.io.InputStream
import java.net.URL

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object App {
  def main(args: Array[String]): Unit = {
    val url: URL = getClass.getResource("/ap.txt")
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(url.toString)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
