package com.ishumei.case_analysis

import com.ishumei.util.SparkUtils

import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 
  */
object GetBadCaseFeatures {
  def main(args: Array[String]): Unit = {
    processCase()
  }

  def processCase(): Unit = {
    val source = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\caseAnalysis\\badcase", "UTF-8")
    val lineIterator = source.getLines


    for (l <- lineIterator) {
      val info = l.split(",")
      println(info(0) + "\001" + info(1) + "\001" + info(2) + "\001" + "-1")
    }
  }

  def getBadCaseFeatures(): Unit = {
    val spark = SparkUtils.getSparkSession("getBadCaseFeatures")

    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.Row
    val schema = StructType(
      StructField("organization", StringType, true) ::
        StructField("token", StringType, true) ::
        StructField("features", StringType, true) ::
        StructField("label", StringType, true) ::
        StructField("requestId", StringType, true) ::
        Nil
    )

    val features = spark.sparkContext.textFile("/user/data/tianwang/niujian/ads_score_v0.2/evaluation/process_features/dt=20190815").map(row => {
      val line = row.split("\001")
      Row(line(0), line(1), line(2), line(3), line(4))
    })

    val featureDF = spark.createDataFrame(features, schema)
    featureDF.createOrReplaceTempView("tmp_feature")

    val whiteSchema = StructType(
      StructField("requestId", StringType, true) ::
        Nil
    )
    val whiteSample = spark.sparkContext.textFile("/user/data/tianwang/niujian/ads_score_v0.2/badcases").map(line => {
      Row(line)
    })

    val whiteDF = spark.createDataFrame(whiteSample, whiteSchema)
    whiteDF.createOrReplaceTempView("tmp_white")

    spark.sql(
      """
        |select
        |a.organization,
        |a.token,
        |a.features
        |from tmp_feature as a
        |join tmp_white as b
        |on a.requestId=b.requestId
      """.stripMargin).write.csv("/user/data/tianwang/niujian/ads_score_v0.2/badcase/dt=20190815")
  }
}
