package com.qf.bigdata.release.etl.release.dm

import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ArrayBuffer

object DMAllHelper {

  /**
    * 渠道用户统计
    */
  def selectDMReleaseCustomerColumns():StringBuffer={
    val columns = new StringBuffer()
    columns.append("sources")
    columns.append("channels")
    columns.append("device_type")
    columns.append("bdp_day")
    columns
  }
}
