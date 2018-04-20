package com.hbc.etl.dim_brand

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest
import com.hbc.etl.dim_brand.staging.{Stage1LastIds, Stage2Brand}
import com.hbc.etl.dim_brand.system.structures.{Audit, Brand, BrandHist}
import com.hbc.etl.dim_brand.system.{Broadcaster, Parameters}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class TestApplication extends FunSuite with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  var fs: FileSystem = _

  val AUDIT_LOG_PATH = "src/test/resources/input/audit_transformation_log.csv"
  val DIM_BRANDS_PATH = "src/test/resources/input/dim_brands.csv"
  val DIM_BRANDS_HIST_PATH = "src/test/resources/input/dim_brands_hist.csv"
  val DIM_RAW_BRANDS_PATH = "src/test/resources/input/dim_raw.csv"

  val ETALON_DIM_BRANDS_PATH = "src/test/resources/etalon/etalon_dim_brands.csv"


  var parameters: Parameters = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Test application").master("local[*]").getOrCreate()
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val ssmClient = AWSSimpleSystemsManagementClientBuilder.standard().withRegion("us-east-1").build()
    val asterUsername = ssmClient.getParameter(new GetParameterRequest().withWithDecryption(true).withName("ASTER_UAT_USERNAME")).getParameter.getValue
    val asterPassword = ssmClient.getParameter(new GetParameterRequest().withWithDecryption(true).withName("ASTER_UAT_PASSWORD")).getParameter.getValue
    val asterUrl = ssmClient.getParameter(new GetParameterRequest().withWithDecryption(false).withName("ASTER_UAT_EXTERNAL_URL")).getParameter.getValue
    val profileCredentials = new ProfileCredentialsProvider().getCredentials

    val args: Array[String] = Array[String](
      "DIM_AUDIT_LOG=audit_transformation_log",
      "DIM_BRANDS=dim_brands",
      "DIM_BRANDS_HIST=dim_brands_hist",
      s"ASTER_URL=$asterUrl",
      s"ASTER_PASSWORD=$asterPassword",
      s"ASTER_USERNAME=$asterUsername"
    )

    parameters = Parameters.getInstance(args)
    TestUtils.createTableFromFile(spark, AUDIT_LOG_PATH, Audit.struct, parameters.DIM_AUDIT_LOG)
    TestUtils.createTableFromFile(spark, DIM_BRANDS_PATH, Brand.struct, parameters.DIM_BRANDS)
    TestUtils.createTableFromFile(spark, DIM_BRANDS_HIST_PATH, BrandHist.struct, parameters.DIM_BRANDS_HISTORY)
    TestUtils.createTableFromFile(spark, ETALON_DIM_BRANDS_PATH, Brand.struct, "dim_brands_etalon")

  }

  test("Testing stage1 and stage 2"){
    implicit val sparkSession: SparkSession = spark
    import sparkSession.implicits._
    val broadcaster = sparkSession.sparkContext.broadcast(Broadcaster(Array()))

    val auditDataFrame = spark.read.table(parameters.DIM_AUDIT_LOG).cache()
    val stg1BrandHistoryId = Stage1LastIds.executeUnprocessedHistory(Seq(auditDataFrame), parameters, broadcaster)
    val stg1BrandIdAndHist = Stage1LastIds.executeUnprocessed(Seq(auditDataFrame), parameters, broadcaster)
    auditDataFrame.unpersist()

    stg1BrandHistoryId should be > 0
    stg1BrandIdAndHist._1 should be > 0
    stg1BrandIdAndHist._2 should be > 0

    val dimBrandHistory = spark.read.table(parameters.DIM_BRANDS_HISTORY).cache()
    val dimBrand= spark.read.table(parameters.DIM_BRANDS).cache()

    val stg2Brand = Stage2Brand.executeBrands(Seq(dimBrandHistory, dimBrand), stg1BrandIdAndHist, parameters, broadcaster)

    if(fs.exists(new Path("src/test/resources/output/dim_brands"))){
      fs.delete(new Path("src/test/resources/output/dim_brands"), true)
    }
    stg2Brand.coalesce(1).write.option("delimiter",";").csv("src/test/resources/output/dim_brands")

    val expected = sparkSession.table("dim_brands_etalon")
    assert(stg2Brand.orderBy($"brand_id",$"ops_brand_id",$"brand_name").collect().toSeq
      == expected.orderBy($"brand_id",$"ops_brand_id",$"brand_name").collect().toSeq)
  }

}
