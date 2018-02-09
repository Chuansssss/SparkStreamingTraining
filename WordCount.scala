package com.jc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark streaming socket data operation
  *
  * Test: nc
  *
  * Author: JcShang
  * 09/02/2018
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    //
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");


    /**
      * two parameters are needed for creating a streaming context: conf and interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5));

    val lines = ssc.socketTextStream("localhost", 6789); // listening on port 6789 by socket, return a DStream
    //val lines = ssc.textFileStream("/Users/JcShang/Desktop/monitor"); // monitor a file directory, file must be moved from another source to the monitored directory

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _); // word count

    result.print();

    ssc.start();
    ssc.awaitTermination();


  }
}
