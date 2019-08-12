package com.ishumei.util

import org.apache.spark.sql.SparkSession

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: spark工具类
  */
object SparkUtils {
  def getSparkSession(appName: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.parquet.compression.codec", "GZIP")
      .config("compression", "gzip")
      .config("spark.debug.maxToStringFields", "10000")
      .config("spark.sql.shuffle.partitions", 200)
      .enableHiveSupport()
      .getOrCreate()
    sparkSession
  }
}
