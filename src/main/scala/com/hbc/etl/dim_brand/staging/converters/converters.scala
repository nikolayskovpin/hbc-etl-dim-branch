package com.hbc.etl.dim_brand.staging

import com.hbc.etl.dim_brand.staging.converters.AdditionalFunctions
import org.apache.spark.sql.DataFrame

package object converters {
  implicit private def toAdditionalFunctions(dataFrame: DataFrame): AdditionalFunctions =
    AdditionalFunctions(dataFrame)
}