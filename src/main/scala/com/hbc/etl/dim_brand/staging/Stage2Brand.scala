package com.hbc.etl.dim_brand.staging

import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object Stage2Brand {
  private val logger = Logger.getLogger(getClass.getName)

  def executeBrands(inputFrames: Seq[DataFrame], lastTuple: (Int, Int), parameters: Parameters, broadcaster: Broadcast[Broadcaster])(implicit spark: SparkSession): DataFrame = {
    logger.info("Stage 2 has started")
    import spark.implicits._

    val (lastId, lastHistId) = lastTuple

    val (dimBrandHistory, dimBrands) = inputFrames match {
      case Seq(frame1, frame2) => (frame1.alias("hist"), frame2.alias("db"))
    }

    //deleting rows
    val dim = dimBrands.join(broadcast(dimBrandHistory), $"hist.brand_id" === $"db.brand_id", "left")
        .filter($"hist.to_time" =!= "3000-01-01 00:00:00+00")
        .filter($"hist.dw_batch_id" <= lastId)
        .select("db.*")

    //rows for insert
    val insert = dimBrandHistory.select($"brand_id",
        $"ops_brand_id",
        $"brand_name",
        $"ups_account_number",
        $"deleted_at",
        $"short_code",
        $"round_msrp",
        $"show_brand_code",
        $"ops_admin_vendor_id",
        $"dw_batch_id",
        $"created_at",
        $"updated_at",
        $"archived_at")
      .where($"dw_batch_id" > lastId)
      .where($"to_time" === "3000-01-01")

    dim.union(insert)
  }

}
