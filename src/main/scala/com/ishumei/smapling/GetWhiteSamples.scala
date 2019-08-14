package com.ishumei.smapling

import com.ishumei.util.SparkUtils

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 补充白样本
  *       1. 广告分 大于 500
  *       2. risklevel='PASS'且内容无明显广告
  *       3. token不在已标注黑账号中
  */
object GetWhiteSamples {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("GetWhiteSamples")

    val date = "dt=20190811"
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    val samplesSchema = StructType(
      StructField("requestId", StringType, true) ::
        StructField("organization", StringType, true) ::
        StructField("tokenId", StringType, true) ::
        StructField("risklevel", StringType, true) ::
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

    val whiteSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT")
    // val whiteSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT/POST_TEXT#part-01023.gz")
    whiteSamples.printSchema()
    whiteSamples.createOrReplaceTempView("whiteSamples")

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
        |risklevel,
        |features.`advertise.score` AS advertiseScore,
        |processText(data.text) AS text
        |FROM whiteSamples
        |WHERE features.`advertise.score`>500
        |AND risklevel='PASS'
      """.stripMargin)
    samplesResult.printSchema()

    // 0?????????????
    samplesResult.write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/whiteSamples/$date")

    samplesResult.sample(false,0.1).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/whiteSample_0.1/$date")
  }
}