package com.hbc.etl.dim_brand.system

import java.util.Properties

import org.apache.log4j.Logger

object Parameters {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def getInstance(args: Array[String]) = new Parameters(args)

  def showParameters(parametersMap: Map[String, String]): Unit ={
    logger.info("**********************************************")
    parametersMap.foreach(item =>
      logger.info(s"* ${item._1} = ${item._2}")
    )
    logger.info("**********************************************")
  }
}

class Parameters(args: Array[String]) {
  private val parametersMap = args.map(setKeyValue).toMap
  private val emptyParameter = "null"
  private val asterUrl = parametersMap.getOrElse("ASTER_URL", emptyParameter)
  private val asterPassword = parametersMap.getOrElse("ASTER_PASSWORD", emptyParameter)
  private val asterUsername = parametersMap.getOrElse("ASTER_USERNAME", emptyParameter)

  val asterJdbcConnection = s"jdbc:ncluster://$asterUrl:2406/dw_customers"
  val asterProps = new Properties()
  asterProps.setProperty("user", asterUsername)
  asterProps.setProperty("password", asterPassword)
  asterProps.setProperty("driver", "com.asterdata.ncluster.Driver")

  val STAGE: Any = parametersMap.getOrElse("STAGE", 0L)
  val DIM_AUDIT_LOG: String = parametersMap.getOrElse("DIM_AUDIT_LOG", emptyParameter)
  val DIM_BRANDS: String = parametersMap.getOrElse("DIM_BRANDS", emptyParameter)
  val DIM_BRANDS_RAW: String = parametersMap.getOrElse("DIM_BRANDS_ROW", emptyParameter)
  val DIM_BRANDS_HISTORY: String = parametersMap.getOrElse("DIM_BRANDS_HISTORY", emptyParameter)

  private def setKeyValue(arg: String): (String, String) = {
    val keyValue = arg.split("=", -1)
    (keyValue(0), keyValue(1))
  }
  Parameters.showParameters(parametersMap)
}
