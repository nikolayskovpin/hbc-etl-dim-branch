package com.hbc.etl.dim_brand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object TestUtils {

  def createTableFromFile(spark: SparkSession, file: String, structType: StructType, tableName: String, delimiter: String = ";"): Unit = spark
    .read
    .format("com.databricks.spark.csv")
    .options(
      Map[String, String](
        "delimiter" -> delimiter,
        "nullValue" -> "",
        "header" -> "false"
      )
    )
    .schema(structType)
    .load(file)
    .createTempView(tableName)
}
