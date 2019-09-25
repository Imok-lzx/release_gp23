package com.qf.bigdata.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW层 投放业务列表
  */
object DWReleaseColumnsHelper {
  /**
    * 目标用户
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]{
    val columns= new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$. idcard') as idcard")
    columns.+=("(cast(date_format(now(),' yyyy') as int)-cast(regexp_extract(get_jsc
    columns.+=("cast(regexp extract(get json object(exts,'$. idcard ),'(\\d{16})(\\d
    columns.+=("get_json _object(exts,'$. area_code') as area_code")
    columns.+=("get _json _object(exts,'$. longitude') as Longitude")
    columns.+=("get_json _object(exts,'$. latitude') as latitude")
    columns.+=("get_json _object(exts,'$. matter_id') as matter_id")
    columns.+=("get _json _object(exts,'$. model_code') as model_code")
    columns.+=("get json _object(exts,'$. model _version') as model_version")
    columns.+=("get_json_object(exts,'$. aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
  }
}
