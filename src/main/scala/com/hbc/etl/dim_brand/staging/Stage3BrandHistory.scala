package com.hbc.etl.dim_brand.staging

import java.sql.{Date, Timestamp}

import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, lit, _}


object Stage3BrandHistory {
  private val logger = Logger.getLogger(getClass.getName)


  def executeBrandsHistry(inputFrames: Seq[DataFrame], lastTuple: (Int, Int), parameters: Parameters, broadcaster: Broadcast[Broadcaster])(implicit spark: SparkSession): DataFrame = {
    logger.info("Stage 3 has started")
    import spark.implicits._

    val (brandsHistory, brandsRaw) = inputFrames match {
      case Seq(frame1, frame2) => (frame1.alias("db"), frame2.alias("bc"))
    }

    val (lastId, lastHistId) = lastTuple

    val brandsNewUpdated = brandsRaw
      .join(broadcast(brandsHistory), $"db.ops_brand_id" === $"bc.id"
        and md5($"bc.id"
        or coalesce(trim(lower($"bc.name")), lit("-1"))
        or coalesce($"bc.ups_account_number", lit("0"))
        or coalesce($"bc.deleted_at", lit("01-01-1000"))
        or coalesce($"bc.short_code", lit("-1"))
        or coalesce($"bc.round_msrp", lit(-1))
        or coalesce($"bc.show_brand_code", lit(-1))
        or coalesce($"bc.admin_vendor_id", lit(-1))
        or coalesce($"bc.archived_at", lit("01-01-1000"))
        or coalesce($"bc.show_brand_code", lit(-1))
        or coalesce($"bc.show_brand_code", lit(-1))
        or coalesce($"bc.show_brand_code", lit(-1))
        or coalesce($"bc.show_brand_code", lit(-1))) ===
        md5($"db.ops_brand_id"
        or coalesce($"db.brand_name", lit("-1"))
        or coalesce($"db.ups_account_number", lit("0"))
        or coalesce($"db.deleted_at", lit("01-01-1000"))
        or coalesce($"db.short_code", lit("-1"))
        or coalesce($"db.round_msrp", lit(-1))
        or coalesce($"db.show_brand_code", lit(-1))
        or coalesce($"db.ops_admin_vendor_id", lit(-1))
        or coalesce($"db.archived_at", lit("01-01-1000")))
        and $"db.to_time" ==="3000-01-01 00:00:00+00"
        and $"bc.dw_batch_id" === lastId
        and $"db.brand_id" isNull
      ).select($"id" as "ops_brand_id",
      trim(lower($"")) as "brand_name",
      $"ups_account_number",
      $"deleted_at",
      $"short_code",
      $"round_msrp",
      $"show_brand_code",
      $"admin_vendor_id" as "ops_admin_vendor_id",
      $"updated_at" as "from_time",
      to_timestamp(lit(Date.valueOf("3000-01-01"))) as "to_time",
      $"dw_batch_id",
      $"created_at",
      $"updated_at",
      $"archived_at"
    )

    val window = Window.partitionBy("ops_brand_id").orderBy("brand_hist_id")

    val df = brandsNewUpdated.withColumn("seq", row_number() over window)
      .withColumn("to_time_cal", lead($"updated_at".as[Timestamp] - expr("INTERVAL 1 MILLISECONDS"), 1, to_timestamp(lit(Date.valueOf("3000-01-01")))) over window)
      .withColumn("updated_at", lag($"brand_id",1, "-1") over window)
      .withColumn("type1", lit("n"))
      .alias("nu")
      .as[BrandsNew]


    //TODO join -> if -> reduce 1
    df.toDF()
  }

  case class BrandsNew(ops_brand_id: String,
                       brand_name:String,
                       ups_account_number: String,
                       deleted_at: String,
                       short_code: String,
                       round_msrp: String,
                       show_brand_code: String,
                       ops_admin_vendor_id: String,
                       to_time: Timestamp,
                       dw_batch_id: String,
                       created_ad: String,
                       archived_at: String,
                       seq: Long,
                       to_time_cal: Timestamp,
                       updated_at: Timestamp,
                       type1 : String
                      )


}
