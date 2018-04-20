package com.hbc.etl.dim_brand.system.structures

import org.apache.spark.sql.types._

object Audit {

  val struct = StructType(
    StructField("id_batch", IntegerType, nullable = false) ::
      StructField("transname", StringType, nullable = false) ::
      StructField("status", StringType, nullable = false) ::
      StructField("lines_read", LongType, nullable = false) ::
      StructField("line_written", LongType, nullable = false) ::
      StructField("line_updated", LongType, nullable = false) ::
      StructField("line_input", LongType, nullable = false) ::
      StructField("line_output", LongType, nullable = false) ::
      StructField("errors", LongType, nullable = false) ::
      StructField("startdate", TimestampType, nullable = false) ::
      StructField("enddate", TimestampType, nullable = false) ::
      StructField("logdate", TimestampType, nullable = false) ::
      StructField("depdate", TimestampType, nullable = false) ::
      StructField("replaydate", TimestampType, nullable = false) ::
      StructField("log_field", StringType, nullable = false) ::
      Nil
  )

}
