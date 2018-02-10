package com.jc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Block list filter
  * Author: JcShang
  * 09/02/2018
  */
object TransformFilterApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");


    /**
      * two parameters are needed for creating a streaming context: conf and interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5));

    /**
      * construct the block list
      */
    val blocks = List("zs", "ls"); // this is usually stored in database
    // map the blocked list into the format (zs, true), (ls, true)
    val blockRDD = ssc.sparkContext.parallelize(blocks).map(x => (x, true))


    val lines = ssc.socketTextStream("localhost", 6789); // return a DStream
    // input data is formatted like this: 20180205,ww
    // left join and filter by whether the flag is true or not
    // then map the accepted log into (ww: [<9292929, ww>, <false>])
    val acceptedLog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blockRDD).filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _); // word count

    acceptedLog.print();

    ssc.start();
    ssc.awaitTermination();
  }
}
