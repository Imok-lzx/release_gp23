package com.qf.bigdata.release.constant

import org.apache.spark.storage.StorageLevel

/**
  * 常量
  */
object ReleaseConstant {

  // partition
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITION = 4


  // 维度列
  val COL_RLEASE_SESSION_STATUS:String = "release_status"
  val COL_RELEASE_SOURCE="sources"
  val COL_RELEASE_CHANNELS="channels"
  val COL_RELEASE_DECICE_TYPE="device_type"
  val COL_RELEASE_DECICE_NUM= "device_num"
  val COL_RELEASE_RELEASE_SESSION="release_session"
  val COL_RELEASE_IDCARD="idcard"
  val COL_RELEASE_USER_COUNT="user_count"
  val COL_RELEASE_TOTAL_COUNT="total_count"
  val COL_RELEASE_AGE_RANGE="age_range"
  val COL_RELEASE_GENDER="gender"
  val COL_RELEASE_AREA_CODE="area_code"
  // ods================================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  // dw=================================
  //目标
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
//曝光
 val DW_RELEASE_EXPOSURE= "dw_release.dw_release_exposure"
//  注册
  val DW_RELEASE_USER="dw_release.dw_release_register_users"
  //点击
 val  DW_RELEASE_CLICK="dw_release.dw_release_click"
//dm==========================================
  val DM_RELEASR_CUSTOMER_SOURCE ="dm_release.dm_customer_sources"
val DM_RELEASE_CUSTOMER_CUBE="dm_release.dm_customer_cube"
}
