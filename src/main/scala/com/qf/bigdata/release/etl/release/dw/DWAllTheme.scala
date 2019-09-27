package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.util.SparkHelper
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object DWAllTheme {
  // 日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWAllTheme.getClass)
  /**
    * 目标客户
    *  01
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    // 获取当前时间
    val begin = System.currentTimeMillis()
    try{
      // 导入隐式转换
      import org.apache.spark.sql.functions._

      // 设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite
      // 获取日志字段数据
      val customerColumns = DWAllHelper.selectDWReleaseCustomerColumns()
      // 设置条件 当天数据 获取目标客户：01
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")
          === lit(ReleaseStatusEnum.CUSTOMER.getCode))
      val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,customerColumns)
        // 填入条件
        .where(customerReleaseCondition)
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
      println("DWReleaseDF==============目标客户=======================")
      // 打印查看结果
      customerReleaseDF.show(10,false)
      // 目标用户（存储）
      SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      // 任务处理的时长
      println(s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}")
    }
  }


  /***
    * 曝光主题
    */
  def exposureReleaseJobs(sparkSession: SparkSession,appName:String,bdp_date:String): Unit ={
    //获取当前时间
    val begin = System.currentTimeMillis()

    try{
      //导入隐式转换
      import sparkSession.implicits._
      import org.apache.spark.sql.functions._

      //设置缓存级别
      val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      //      获取日志字段
      val exposureColumns: ArrayBuffer[String] = DWAllHelper.selectDWReleaseExposureColumns()
      //     设置条件 当天数据 获取 曝光数据 03
      val exposureReleaseCondition =(col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_date) and col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")===lit(ReleaseStatusEnum.SHOW.getCode))
      val exposureReleaseDF= SparkHelper.readTableData (sparkSession, ReleaseConstant.ODS_RELEASE_SESSION, exposureColumns )
        .where ( exposureReleaseCondition )
        .repartition ( ReleaseConstant.DEF_SOURCE_PARTITION )
      println("DWReleaseDF==============曝光主题=======================")

      // 打印查看结果
      exposureReleaseDF.show(10,false)
      // 曝光（存储）
          SparkHelper.writeTableData(exposureReleaseDF,ReleaseConstant.DW_RELEASE_EXPOSURE,saveMode)

    }catch {
      //      错误信息处理
      case ex:Exception=>{logger.error(ex.getMessage,ex)
      }
    }
    finally {
      //        任务处理时长
      println(s"任务处理时长：${appName},bdp_day = ${bdp_date}, ${System.currentTimeMillis() - begin}")
    }

  }

  /**
    * 注册
    */
  def userReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    // 获取当前时间
    val begin = System.currentTimeMillis()
    try{
      // 导入隐式转换
      import org.apache.spark.sql.functions._

      // 设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite
      // 获取日志字段数据
      val userColumns = DWAllHelper.selectDWReleaseUserColumns()
      // 设置条件 当天数据 获取注册：
      val userReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")
          === lit(ReleaseStatusEnum.REGISTER.getCode))
      val userReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,userColumns)
        // 填入条件
        .where(userReleaseCondition)
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
      println("DWReleaseDF==============注册主题=======================")
      // 打印查看结果
      userReleaseDF.show(10,false)
      // 注册（存储）
        SparkHelper.writeTableData(userReleaseDF,ReleaseConstant.DW_RELEASE_USER,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      // 任务处理的时长
      println(s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}")
    }
  }


    /**
      * 点击
      *  04
      */
    def clickReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
      // 获取当前时间
      val begin = System.currentTimeMillis()
      try{
        // 导入隐式转换
        import org.apache.spark.sql.functions._

        // 设置缓存级别
        val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
        val saveMode = SaveMode.Overwrite
        // 获取日志字段数据
        val clickColumns = DWAllHelper.selectDWReleaseClickColumns()
        // 设置条件 当天数据 获取注册：04
        val clickReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
          and
          col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")
            === lit(ReleaseStatusEnum.CLICK.getCode))
        val clickReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,clickColumns)
          // 填入条件
          .where(clickReleaseCondition)
          // 重分区
          .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
        println("DWReleaseDF==============点击主题=======================")
        // 打印查看结果
        clickReleaseDF.show(10,false)
        // 点击（存储）
        SparkHelper.writeTableData(clickReleaseDF,ReleaseConstant.DW_RELEASE_CLICK,saveMode)

      }catch {
        // 错误信息处理
        case ex:Exception =>{
          logger.error(ex.getMessage,ex)
        }
      }finally {
        // 任务处理的时长
        println(s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}")      }
    }


}
