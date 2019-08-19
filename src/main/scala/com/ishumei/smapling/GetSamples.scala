package com.ishumei.smapling

import java.io.PrintWriter

import com.ishumei.util.SparkUtils

import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 样本抽样
  *     1. 黑样本：广告分大于500，抽样10%，人工标注内容为黑
  *     2. 白样本：result.rule-engine.riskLevel='PASS'，抽样0.1%，人工标注内容为白
  */
object GetSamples {
  def main(args: Array[String]): Unit = {
    getSampleReqOrgToken()
  }

  def getSampleReqOrgToken(): Unit = {
    val source = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\样本抽取\\whiteSamples.csv", "UTF-8")
    val lineIterator = source.getLines

    val out = new PrintWriter("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\样本抽取\\whiteSampleReq.csv")

    for (l <- lineIterator) {
      val info = l.split(",")
      out.println(info(0) + "," + info(1) + "," + info(2) + "," + "0")
    }

    out.close()
  }

  def getBlackSamples(): Unit = {
    val spark = SparkUtils.getSparkSession("getBlackSamples")

    val date = "dt=20190811"
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    val blackSamplesSchema = StructType(
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

    val blackSamples = spark.read.schema(blackSamplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT")
    // val allSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT/POST_TEXT#part-01023.gz")
    blackSamples.createOrReplaceTempView("blackSamples")

    def processText(text: String): String = {
      val str = text.replace("\n\r", "~")
      str.replace("\n", "~")
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
        |FROM blackSamples
        |WHERE features.`advertise.score`>500
        |ORDER BY organization,tokenId
      """.stripMargin)
    samplesResult.printSchema()

    // requestId,organization,tokenId,advertiseScore,text
    // 所有广告分大于500的事件
    samplesResult.sample(false, 0.1).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/blackSamples/$date")
  }

  def getWhiteSamples(): Unit = {
    val spark = SparkUtils.getSparkSession("GetWhiteSamples")

    val date = "dt=20190811"
    import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}
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
        StructField("result", StructType(
          StructField("rule-engine", StructType(
            StructField("riskLevel", StringType, true) ::
              Nil
          ), true) ::
            Nil), true
        ) ::
        Nil
    )

    def processText(text: String): String = {
      val str = text.replace("\n\r", "~")
      str.replace("\n", "~")
    }

    spark.udf.register("processText", processText _)

    val whiteSamples = spark.read.schema(samplesSchema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT/")
    whiteSamples.createOrReplaceTempView("whiteSamples")

    val samplesResult = spark.sql(
      """
        |SELECT
        |requestId,
        |organization,
        |tokenId,
        |result.`rule-engine`.riskLevel AS riskLevel,
        |features.`advertise.score` AS advertiseScore,
        |processText(data.text) AS text
        |FROM whiteSamples
        |WHERE result.`rule-engine`.riskLevel='PASS' AND features.`advertise.score`>500
        |ORDER BY organization,tokenId
      """.stripMargin)

    samplesResult.printSchema()

    samplesResult.count() // 1310350
    samplesResult.write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2.1/whiteSamples/$date")

    val result = spark.read.csv(s"/user/data/tianwang/niujian/ads_score_v0.2.1/whiteSamples/$date")
    result.sample(false,0.1).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2.1/whiteSamples_0.1/$date")
  }
}