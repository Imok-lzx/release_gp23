package com.qf.bigdata.release.etl.release.dw

//import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.{handleJobs, logger}
import com.qf.bigdata.release.etl.release.dm.DMCustomerSources
import com.qf.bigdata.release.etl.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
object DMReleaseMain {
  private val logger: Logger = LoggerFactory.getLogger ( ReleaseMain.getClass )

  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark:SparkSession =null
    try{
      // 配置Spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")

      // 创建上下文
      spark = SparkHelper.createSpark(conf)
      spark.sparkContext.setLogLevel("Error")

      // 解析参数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      // 循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        DMCustomerSources.ChannelUserStatistics(spark,appName,bdp_date)

      }
    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val appName = "dw_release_job"
    val bdp_day_begin = "2019-09-24"
    val bdp_day_end = "2019-09-24"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }



}
