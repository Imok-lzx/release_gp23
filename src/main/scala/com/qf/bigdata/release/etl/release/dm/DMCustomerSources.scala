package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.release.dw.{DWAllHelper, DWAllTheme}
import com.qf.bigdata.release.etl.util.SparkHelper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object DMCustomerSources {
  // 日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWAllTheme.getClass)
  /**
    * 渠道用户统计
    *  01
    */
  def ChannelUserStatistics(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    // 获取当前时间
    val begin = System.currentTimeMillis()
    try{
      // 导入隐式转换
      import org.apache.spark.sql.functions._

      // 设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite
      // 获取日志字段数据
      val customerColumns = DMAllHelper.selectDMReleaseCustomerColumns()
      // 设置条件 当天数据 获取目标客户：01
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day))
      val customerReleaseDF = SparkHelper.readTableData1(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns,customerReleaseCondition)
        // 填入条件
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
      println("DWReleaseDF==============渠道统计=======================")
      // 打印查看结果
      customerReleaseDF.show(10,false)
      // 目标用户（存储）
//      SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      // 任务处理的时长
      s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}"
    }
  }
}
