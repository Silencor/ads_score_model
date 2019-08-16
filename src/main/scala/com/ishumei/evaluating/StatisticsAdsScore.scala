package com.ishumei.evaluating

import com.ishumei.util.SparkUtils

object StatisticsAdsScore {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("StatisticsAdsScore")

    import org.apache.spark.sql.types._
    val adsScore_schema = StructType(
      StructField("organization", StringType, true) ::
        StructField("token", StringType, true) ::
        StructField("pred", DoubleType, true) ::
        StructField("timestamp", LongType, true) ::
        StructField("requestId", StringType, true) ::
        Nil
    )

    val date = "dt=20190815"
    val adsScore = spark.read.schema(adsScore_schema).option("delimiter", "\t").csv(s"/user/data/tianwang/niujian/ads_score_v0.2/evaluation/predict_output/$date")
    adsScore.createOrReplaceTempView("adsScore")

    adsScore.write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/adsScore/$date")

    adsScore.count() //8246496
    adsScore.where("pred>0.95 and pred<0.96").count() // 459429
    adsScore.where("pred>=0.96 and pred<0.97").count() // 336575
    adsScore.where("pred>=0.97 and pred<0.98").count() // 44832,7650
    adsScore.where("pred>=0.98 and pred<0.99").count() // 98
    adsScore.where("pred>=0.99 and pred<1").count() // 0

    // 各阈值下，token打分最大值
    val sql =
      """
        |select
        |requestId,
        |organization,
        |token,
        |pred
        |from (select organization, token, requestId, pred, row_number()over(partition by organization, token order by pred desc) as rank
        |from adsScore where pred>=0.96 and pred<0.97)
        |where rank=1""".stripMargin

    val result = spark.sql(sql)
    result.count()
    result.sample(false, 0.1).limit(100).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/adsScore/p3/$date")

    // 各阈值下，抽样200
    val adsScore_sample = spark.read.csv("/user/data/tianwang/niujian/ads_score_v0.2/adsScoreWithDifPred/p5")
    adsScore_sample.sample(false, 0.5).limit(200).write.csv("/user/data/tianwang/niujian/ads_score_v0.1/adsScoreWithDifPredSample/p5")
  }
}