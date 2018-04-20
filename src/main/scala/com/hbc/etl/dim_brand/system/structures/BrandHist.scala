package com.hbc.etl.dim_brand.system.structures

import org.apache.spark.sql.types._

object BrandHist {

  val struct = StructType(
    StructField("brand_hist_id", LongType, nullable = false) ::
      StructField("brand_id", LongType, nullable = false) ::
      StructField("ops_brand_id", IntegerType, nullable = false) ::
      StructField("brand_name", StringType, nullable = false) ::
      StructField("ups_account_number", StringType, nullable = false) ::
      StructField("deleted_at", TimestampType, nullable = false) ::
      StructField("short_code", IntegerType, nullable = false) ::
      StructField("round_msrp", ShortType, nullable = false) ::
      StructField("show_brand_code", ShortType, nullable = false) ::
      StructField("ops_admin_vendor_id", IntegerType, nullable = false) ::
      StructField("from_time", TimestampType, nullable = false) ::
      StructField("to_time", TimestampType, nullable = false) ::
      StructField("dw_batch_id", IntegerType, nullable = false) ::
      StructField("created_at", TimestampType, nullable = false) ::
      StructField("updated_at", TimestampType, nullable = false) ::
      StructField("archived_at", TimestampType, nullable = false) ::
      Nil
  )

}
