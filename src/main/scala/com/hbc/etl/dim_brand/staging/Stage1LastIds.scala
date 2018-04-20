package com.hbc.etl.dim_brand.staging

import com.hbc.etl.dim_brand.staging.converters.AdditionalFunctions
import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Stage1LastIds {
  private val logger = Logger.getLogger(getClass.getName)
  implicit val toAdditionalFunctions: (DataFrame) => AdditionalFunctions = toAdditionalFunctions

  def executeUnprocessedHistory(inputFrames: Seq[DataFrame], parameters: Parameters, broadcaster: Broadcast[Broadcaster])(implicit spark: SparkSession): Int = {
    logger.info("Stage 1 has started")
    import spark.implicits._

    val auditLog = inputFrames match {
      case Seq(frame) => frame
    }

    val lastId = auditLog
      .where($"transname" === "gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist" and $"status" === "end")
      .select(coalesce(max($"id_batch"), lit(-1)) as "max_clean_batch_id")
      .first()
      .getInt(0)

    val currentDataFrame = auditLog
      .alias("d1")
      .where($"d1.id_batch" > lastId)
      .where($"d1.transname" === "gilt_us_ops#IOE_set_time_bounds_value#raw_inc##")
      .where($"d1.status" === "end")
      .select($"id_batch" as "dw_batch_id", lit("gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist") as "transname_target")
      .distinct()
      .orderBy($"id_batch")

    currentDataFrame.select($"dw_batch_id").first().getInt(0)
  }

  def executeUnprocessed(inputFrames: Seq[DataFrame], parameters: Parameters, broadcaster: Broadcast[Broadcaster])(implicit spark: SparkSession): (Int, Int) = {
    import spark.implicits._

    val auditLog = inputFrames match {
      case Seq(frame) => frame
    }

    val lastId = auditLog
      .where($"transname" === "gilt_us_raw_inc_ops#dim_brands#dimension#dw.dim_brands" and $"status" === "end")
      .select(coalesce(max($"id_batch"), lit(-1)))
      .first()
      .getInt(0)

    val lastIdHist = auditLog
      .alias("d1")
      .where($"d1.id_batch" < lastId)
      .where($"d1.transname" === "gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist")
      .where($"d1.status" === "end")
      .select(max($"id_batch"))
      .first()
      .getInt(0)


    (lastId, lastIdHist)
  }

}
