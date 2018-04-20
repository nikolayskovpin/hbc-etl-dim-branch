package com.hbc.etl.dim_brand.staging.converters

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, max}

case class AdditionalFunctions(dataFrame: DataFrame) {

  def getLastId(transname1: String, transname2: String) : (Int, Int) = {
    val persisted = dataFrame.cache()

    val lastId = persisted
      .where(col("transname") === transname1)
      .where(col("status") === "end")
      .select(coalesce(max(col("id_batch")), lit(-1)))
      .first()
      .getInt(0)

    val loadId: Int =
      persisted
        .where(col("transname") === transname2)
        .where(col("status") === "end")
        .where(col("id_batch") > lastId)
        .select(max(col("id_batch")))
        .first()
        .getInt(0)

    persisted.unpersist()
    (lastId, loadId)
  }

}
