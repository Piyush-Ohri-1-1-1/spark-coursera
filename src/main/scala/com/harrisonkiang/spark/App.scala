package com.harrisonkiang.spark

/**
 * A self-contained Spark application to estimate the value of pi
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object App {

  def main(args: Array[String]): Unit = {
    val NUM_SAMPLES = 1000000000
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local[4]") // adjust to number of cores
    val sc = new SparkContext(conf)
    val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
      val x = Math.random()
      val y = Math.random()
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)

  }
}
