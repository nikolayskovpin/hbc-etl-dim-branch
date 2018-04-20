package com.hbc.etl.dim_brand

import com.hbc.etl.dim_brand.staging.StagingRunner
import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Starter{
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Application has started")

    val parameters = Parameters.getInstance(args)
    val spark = SparkSession.builder().getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val broadcaster = spark.sparkContext.broadcast(Broadcaster(Array()))

    StagingRunner.run(spark, parameters, broadcaster)

    spark.stop
  }

}
