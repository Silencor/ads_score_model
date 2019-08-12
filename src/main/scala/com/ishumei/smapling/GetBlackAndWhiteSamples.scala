package com.ishumei.smapling

import com.ishumei.util.SparkUtils

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 黑白样本抽样
  *     1. 选择有文本事件的公司，用文本广告分大于500初筛token
  *     2. 人工标注这些token是不是黑产
  */
object GetBlackAndWhiteSamples {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("GetBlackAndWhiteSamples")

    val date = "dt=20190811"

    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    val samplesSchema = StructType(
      StructField("requestId", StringType, true) ::
        StructField("organization", StringType, true) ::
        StructField("tokenId", StringType, true) ::
        StructField("features", StructType(
          StructField("advertise.score", IntegerType, true) ::
            Nil), true
        ) ::
        StructField("data", StructType(
          StructField("text", StringType, true) ::
            Nil), true
        ) ::
        Nil
    )

    val allSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT")
    // val allSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT/POST_TEXT#part-01023.gz")
    allSamples.printSchema()
    allSamples.createOrReplaceTempView("allSamples")

    def processText(text: String): String = {
      text.replace("\n\r","~").replace("\n", "~")
    }

    spark.udf.register("processText", processText _)

    val samplesResult = spark.sql(
      """
        |SELECT
        |requestId,
        |organization,
        |tokenId,
        |features.`advertise.score` AS advertiseScore,
        |processText(data.text) AS text
        |FROM allSamples
        |WHERE features.`advertise.score`>500
        |ORDER BY organization,tokenId
      """.stripMargin)
    samplesResult.printSchema()

    // requestId,organization,tokenId,advertiseScore,text
    // 所有广告分大于500的事件
    samplesResult.write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/allSamples/$date")
  }
}
