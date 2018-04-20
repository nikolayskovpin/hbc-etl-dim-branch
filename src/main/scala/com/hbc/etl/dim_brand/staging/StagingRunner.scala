package com.hbc.etl.dim_brand.staging

import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StagingRunner {

  def run(spark: SparkSession, parameters: Parameters, broadcaster: Broadcast[Broadcaster]): Unit = {
    implicit val sparkSession: SparkSession = spark

    val auditDataFrame = spark.read.jdbc(parameters.asterJdbcConnection, parameters.DIM_AUDIT_LOG, parameters.asterProps).cache()
    val stg1BrandHistoryId = Stage1LastIds.executeUnprocessedHistory(Seq(auditDataFrame), parameters, broadcaster)
    val stg1BrandIdAndHist = Stage1LastIds.executeUnprocessed(Seq(auditDataFrame), parameters, broadcaster)
    auditDataFrame.unpersist()

    val dimBrandHistory = spark.read.jdbc(parameters.asterJdbcConnection, parameters.DIM_BRANDS_HISTORY, parameters.asterProps).cache()
    val stg2Brand = Stage2Brand.executeBrands(Seq(dimBrandHistory), stg1BrandIdAndHist, parameters, broadcaster)
    stg2Brand.write.saveAsTable(parameters.DIM_BRANDS_HISTORY)

    val dimBrandRaw = spark.read.jdbc(parameters.asterJdbcConnection, parameters.DIM_BRANDS_RAW, parameters.asterProps).cache()
    val stg3Brand = Stage3BrandHistory.executeBrandsHistry(Seq(dimBrandHistory, dimBrandRaw), stg1BrandIdAndHist, parameters, broadcaster)
    stg3Brand.write.saveAsTable(parameters.DIM_BRANDS_RAW)
  }

}