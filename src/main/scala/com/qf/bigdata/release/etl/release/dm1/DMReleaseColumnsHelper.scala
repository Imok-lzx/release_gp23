package com.qf.bigdata.release.etl.release.dm1

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {

  /**
    * 获取目标客户
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range ")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 渠道指标列
    */
  def selectDMCustomerSourceColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns
  }

/**
  *目标客户多维度分析统社
  */
  def selectDMCustomerCudeColumns()={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range ")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns
  }

}
