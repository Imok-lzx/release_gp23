package com.qf.bigdata.release.etl.release.dm1

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dm.DMCustomerSources
import com.qf.bigdata.release.etl.release.dw.DMReleaseMain.logger
import com.qf.bigdata.release.etl.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 投放目标客户数据及时
  */
object DMRekeaseCustomer {
//日志
  private val logger: Logger = LoggerFactory.getLogger(DMRekeaseCustomer.getClass)
  /**
    * 统计目标客户集市
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String)={
    //导入隐式转换
    import  org.apache.spark.sql.functions._
    import  spark.implicits._
    //缓存级别
    val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
    val saveMode = SaveMode.Overwrite
//    获取日志数据
val customerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
//    获取当天数据
    val customerCondition: Column = col(s"${ReleaseConstant.DEF_PARTITION }") === lit(bdp_day)
    val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
      .where(customerCondition)
      .persist(storagelevel)
     println("DW========================")
    customerReleaseDF.show(10,false)

// 统计渠道指标
   val customerSourceGroupColumns=
     Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCE}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DECICE_TYPE}")
//    插入列方法
    val customerSourceColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMCustomerSourceColumns()
    //按照需求分组，进行聚合
  val  customerSourceDMDF=customerReleaseDF
      .groupBy(customerSourceGroupColumns:_*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_DECICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}" ),
        count(col(ReleaseConstant.COL_RELEASE_DECICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
//按照条件查询  如果获取不到当前列得值，就会把后面的日起复制给这个列
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      //所有维度列 ，封装方法
      .selectExpr(customerSourceColumns:_*)

    println("DM_Source===================")
//    打印
    customerSourceDMDF.show(10,false)
//    存储
//    SparkHelper.writeTableData(customerSourceDMDF,ReleaseConstant.DM_RELEASR_CUSTOMER_SOURCE,saveMode)


    //目标客户多维度统计
    val customerGroupColumns= Seq[Column](
      $"${ ReleaseConstant.COL_RELEASE_SOURCE}",
      $"${ ReleaseConstant. COL_RELEASE_CHANNELS}",
      $"${ ReleaseConstant. COL_RELEASE_DECICE_TYPE}",
      $"${ ReleaseConstant. COL_RELEASE_AGE_RANGE}",
      $"${ ReleaseConstant. COL_RELEASE_GENDER}",
      $"${ ReleaseConstant. COL_RELEASE_AREA_CODE}"
    )
    //插入列
    val customerCubeColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMCustomerCudeColumns()

    //统计聚合
    val  customerCubeDMDF=customerReleaseDF
      .groupBy(customerGroupColumns:_*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_DECICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}" ),
        count(col(ReleaseConstant.COL_RELEASE_DECICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
      //按照条件查询  如果获取不到当前列得值，就会把后面的日起复制给这个列
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      //所有维度列 ，封装方法
      .selectExpr(customerCubeColumns:_*)
    println("DM_Source===================")
    //    打印
    customerCubeDMDF.show(10,false)
  }

}
