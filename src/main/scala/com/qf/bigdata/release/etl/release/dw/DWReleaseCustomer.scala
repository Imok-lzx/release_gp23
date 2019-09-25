package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * DW 投放目标客户主题
  */
object DWReleaseCustomer {
//日志处理

  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)
  /***
    * 目标客户
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String)={
    //获取当前时间
    val begin: Long = System.currentTimeMillis()
    try{
      //导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //设置缓存级别
      val storagelevel: StorageLevel = ReleaseConstant.SEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      //获取日志字段数据

    }
    catch{

    }
  }

}
