package com.ishumei.training

import java.io.PrintWriter

import com.ishumei.util.SparkUtils

import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 获取黑、白样本特征（目前从仍从ae中关联，后续从HBase一级特征读取）
  *     1. 检查并处理样本数据，reqeustId, org, token, label
  *     2. 根据requestId关联ae日志，获取原始61个特征，并组织为：requestId, org, token, features, lable
  *     3. 处理原始特征，将61维扩展为65维，并将数据组织为： org \001 token \001 feature0 \t feature1... \001 label
  *     4. 将数据划分为训练集和测试集，要求token不能重复
  */
object GetBlackAndWhiteFeatures {
  val path = "D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\"

  def main(args: Array[String]): Unit = {
    val blackSamplePath = path + "样本抽取\\blackSample.csv"
    val whiteSamplePath = path + "样本抽取\\whiteSample.csv"

    checkAndProcessSample(blackSamplePath, 1)
    checkAndProcessSample(whiteSamplePath, 0)

    //joinAeFeatures()

    //processOrgFeatures()
  }

  /**
    * 处理原始特征，将61维扩展为65维，并将数据组织为： org \001 token \001 feature0 \t feature1... \001 label
    * 输入数据为原始特征数据reqeustId, org, token, features, label
    * 路径：/user/data/tianwang/niujian/ads_score_v0.2/dataSet/org_features/dt=20190811
    * 输出为org \001 token \001 feature0 \t feature1... \001 label
    * 路径：/user/data/tianwang/niujian/ads_score_v0.2/dataSet/all_features/dt=20190811
    */
  def processOrgFeatures() : Unit = {
    val spark = SparkUtils.getSparkSession("processOrgFeatures")

    import java.util.Date
    val date = "dt=20190811"
    val orgFeatures = spark.sparkContext.textFile(s"/user/data/tianwang/niujian/ads_score_v0.2/org_features/$date/")
    val allFeatures = orgFeatures.map(row => {
      val features = row.split(",")

      val line = new StringBuilder
      if (features.length == 65) {
        // features(0) requestId
        line.append(features(1) + "\001"+ features(2) + "\001")

        val data_type = features(3) // 类型，分成两类（ZHIBO表示直播，其他都是论坛FORUM)
        if ("ZHIBO" == data_type) line.append("01").append("\t")
        else if ("" == data_type) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_channel = features(4) // 渠道, 分成两类（MESSAGE/IM_TEXT表示私信，其它表示公开）
        if ("MESSAGE" == data_channel || "IM_TEXT" == data_channel) line.append("01").append("\t")
        else if ("" == data_channel) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_role = features(5) // 角色，分成两类（NORMAL、SPECIAL）
        if ("NORMAL" == data_role) line.append("01").append("\t")
        else if ("" == data_role) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_sex = features(6) // 性别
        if ("0" == data_sex) line.append("01").append("\t")
        else if ("" == data_sex) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val text_is_all_digit = features(7) // 是否为纯数字
        if ("TRUE" == text_is_all_digit) line.append("0").append("\t")
        else if ("" == text_is_all_digit) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        for (i <- 8 to 19) {
          if ("" == features(i)) line.append("-1.0").append("\t")
          else line.append(features(i)).append("\t")
        }

        val contact_withNicknameResult_has_contact = features(20) // 昵称加文本是否包含联系方式
        if ("TRUE" == contact_withNicknameResult_has_contact) line.append("0").append("\t")
        else if ("" == contact_withNicknameResult_has_contact) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        var enCharacter = 0 // 字母个数
        var numberCharacter = 0 // 数字个数
        val contact_withNicknameResult_wechat_DEFINITENESS = features(21) // 昵称加文本中的联系方式串
        if ("" != contact_withNicknameResult_wechat_DEFINITENESS) { // 计算字母个数，数字个数，总长度
          for (i <- 0 until contact_withNicknameResult_wechat_DEFINITENESS.length) {
            val tmp = contact_withNicknameResult_wechat_DEFINITENESS.charAt(i)
            if (Character.isLetter(tmp)) enCharacter += 1
            else if (Character.isDigit(tmp)) numberCharacter += 1
          }

          line.append(enCharacter).append("\t").append(numberCharacter).append("\t").append(contact_withNicknameResult_wechat_DEFINITENESS.length).append("\t")
          enCharacter = 0
          numberCharacter = 0
        } else {
          line.append("-1.0").append("\t").append("-1.0").append("\t").append("-1.0").append("\t")
        }

        val contact_withNicknameResult_wechat_DISTANCE = features(22) // 昵称加文本中关键词和串的距离
        if ("" == contact_withNicknameResult_wechat_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_withNicknameResult_wechat_DISTANCE).append("\t")

        val contact_withNicknameResult_qq_DISTANCE = features(23) // 昵称加文本中QQ关键词距离串的距离
        if ("" == contact_withNicknameResult_qq_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_withNicknameResult_qq_DISTANCE).append("\t")

        val contact_textResult_has_contact = features(24) // 文本是否包含联系方式
        if ("TRUE" == contact_textResult_has_contact) line.append("0").append("\t")
        else if ("" == contact_textResult_has_contact) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        val contact_textResult_wechat_DEFINITENESS = features(25) // 文本中的联系方式串
        if ("" != contact_textResult_wechat_DEFINITENESS) { // 计算字母个数，数字个数，总长度
          for (i <- 0 until contact_textResult_wechat_DEFINITENESS.length) {
            val tmp = contact_textResult_wechat_DEFINITENESS.charAt(i)
            if (Character.isLetter(tmp)) enCharacter += 1
            else if (Character.isDigit(tmp)) numberCharacter += 1
          }

          line.append(enCharacter).append("\t").append(numberCharacter).append("\t").append(contact_textResult_wechat_DEFINITENESS.length).append("\t")
        } else {
          line.append("-1.0").append("\t").append("-1.0").append("\t").append("-1.0").append("\t")
        }

        val contact_textResult_wechat_DISTANCE = features(26) // 文本中关键词和串的距离
        if ("" == contact_textResult_wechat_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_textResult_wechat_DISTANCE).append("\t")

        val contact_textResult_qq_DISTANCE = features(27) // 文本中QQ关键词距离串的距离
        if ("" == contact_textResult_qq_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_textResult_qq_DISTANCE).append("\t")

        // redis-counter
        for (n <- 28 to 62) {
          if ("" == features(n)) line.append("0").append("\t")
          else line.append(features(n)).append("\t")
        }

        val profile_activity_first_time = features(63) // 账号首次活跃时间，计算到当前时间的时间差
        if ("" == profile_activity_first_time) line.append("-1.0")
        else { // 不为空，计算到当前的时间差
          val date = profile_activity_first_time.toLong
          val nowDate = new Date().getTime
          line.append(nowDate - date)
        }

        // label
        val label = features(64) //标签
        line.append("\001" + label)
      }

      line.toString()
    }).filter(str => !str.equals(""))

    // org \001 token \001 feature0 \t feature1... \001 label
    allFeatures.saveAsTextFile(s"/user/data/tianwang/niujian/ads_score_v0.2/features/$date")

    //-------分割为训练集与测试集
  }

  /**
    * 根据requestId关联ae日志，获取原始61个特征，并组织为：org, token, features, label
    * 输入数据为黑白样本的合并数据，路径为：D:\SM\数美科技-数据挖掘\广告导流\ads_score_v0.2\特征关联\allSet.csv
    * 上传至hdfs：/user/data/tianwang/niujian/ads_score_v0.2/dataSet/allSet.csv
    * 输出数据为关联出原始特征的数据，reqeustId, org, token, features, label
    * 路径为：/user/data/tianwang/niujian/ads_score_v0.2/dataSet/org_features/dt=20190811
    */
  def joinAeFeatures(): Unit = {
    val spark = SparkUtils.getSparkSession("GetBlackAndWhiteFeatures")

    // 读取样本数据
    val allSamples = spark.read.option("header", "true").csv("/user/data/tianwang/niujian/ads_score_v0.2/features/allSamples.csv")
    allSamples.createOrReplaceTempView("allSamples")

    val date = "dt=20190811"
    // 读取ae数据
    import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
    val ae_schema = StructType(
      StructField("requestId", StringType, true) ::
        StructField("organization", StringType, true) ::
        StructField("tokenId", StringType, true) ::
        StructField("type", StringType, true) ::
        StructField("data", StructType(
          StructField("channel", StringType, true) ::
            StructField("role", StringType, true) ::
            StructField("sex", StringType, true) ::
            Nil
        ), true) ::
        StructField("features", StructType(
          StructField("text.is_all_digit", BooleanType, true) ::
            StructField("text.char_num", IntegerType, true) ::
            StructField("text.normalized_char_num", IntegerType, true) ::
            StructField("text.ch_char_num", IntegerType, true) ::
            StructField("text.en_char_num", IntegerType, true) ::
            StructField("text.digit_char_num", IntegerType, true) ::
            StructField("text.unknown_char_num", IntegerType, true) ::
            StructField("text.dup_char_num", IntegerType, true) ::
            StructField("text.chinese_phrase_num", IntegerType, true) ::
            StructField("ngram.text.score", IntegerType, true) ::
            StructField("ngram.chtext.score", IntegerType, true) ::
            StructField("advertise.score", IntegerType, true) ::
            StructField("advertise.nickname.score", IntegerType, true) ::
            StructField("contact.withNicknameResult.has_contact", BooleanType, true) ::
            StructField("contact.withNicknameResult.wechat.DEFINITENESS", StringType, true) ::
            StructField("contact.withNicknameResult.wechat.DISTANCE", IntegerType, true) ::
            StructField("contact.withNicknameResult.qq.DISTANCE", IntegerType, true) ::
            StructField("contact.textResult.has_contact", BooleanType, true) ::
            StructField("contact.textResult.wechat.DEFINITENESS", StringType, true) ::
            StructField("contact.textResult.wechat.DISTANCE", IntegerType, true) ::
            StructField("contact.textResult.qq.DISTANCE", IntegerType, true) ::
            StructField("redis-counter.org_text_count_per_10s", IntegerType, true) ::
            StructField("redis-counter.org_text_count_per_5m", IntegerType, true) ::
            StructField("redis-counter.org_text_count_per_30m", IntegerType, true) ::
            StructField("redis-counter.org_chtext_count_per_10s", IntegerType, true) ::
            StructField("redis-counter.org_chtext_count_per_5m", IntegerType, true) ::
            StructField("redis-counter.org_chtext_count_per_30m", IntegerType, true) ::
            StructField("redis-counter.text_simhash_per_org_token_20c_10s", IntegerType, true) ::
            StructField("redis-counter.text_simhash_per_org_token_100c_5m", IntegerType, true) ::
            StructField("redis-counter.text_simhash_per_org_token_100c_30m", IntegerType, true) ::
            StructField("redis-counter.org_textwechat_count_per_10s", IntegerType, true) ::
            StructField("redis-counter.org_textwechat_count_per_5m", IntegerType, true) ::
            StructField("redis-counter.org_textwechat_count_per_30m", IntegerType, true) ::
            StructField("redis-counter.token_dupcount_textwechat_30m", IntegerType, true) ::
            StructField("redis-counter.text_simhash_per_ip_100c_30m", IntegerType, true) ::
            StructField("redis-counter.ip_content_length_per_10s", IntegerType, true) ::
            StructField("redis-counter.ip_content_length_per_5m", IntegerType, true) ::
            StructField("redis-counter.ip_content_length_per_30m", IntegerType, true) ::
            StructField("redis-counter.ip_count_per_10s", IntegerType, true) ::
            StructField("redis-counter.ip_count_per_10m", IntegerType, true) ::
            StructField("redis-counter.distinct_new_token_curtime_per_ip_3h", IntegerType, true) ::
            StructField("redis-counter.event_cnt_per_token_tailsim_3h", IntegerType, true) ::
            StructField("redis-counter.event_cnt_per_token_midsim_3h", IntegerType, true) ::
            StructField("redis-counter.event_cnt_per_token_headsim_3h", IntegerType, true) ::
            StructField("redis-counter.newtoken_cnt_per_txt_1d", IntegerType, true) ::
            StructField("redis-counter.newtoken_cnt_per_txt_3h", IntegerType, true) ::
            StructField("redis-counter.dup_cnt_per_newtoken_txt_3h", IntegerType, true) ::
            StructField("redis-counter.dup_cnt_per_newtoken_txt_1d", IntegerType, true) ::
            StructField("redis-counter.event_cnt_per_token_receiveToken_1d", IntegerType, true) ::
            StructField("redis-counter.distinct_text_per_token_receiveToken_1d", IntegerType, true) ::
            StructField("redis-counter.token_text_dup_per_30m", IntegerType, true) ::
            StructField("redis-counter.distinct_text_per_token_receiveToken_30m", IntegerType, true) ::
            StructField("redis-counter.chtext_count_per_token_30m", IntegerType, true) ::
            StructField("redis-counter.distinct_text_per_token_30m", IntegerType, true) ::
            StructField("redis-counter.token_count_per_10s", IntegerType, true) ::
            StructField("redis-counter.token_count_per_1d", IntegerType, true) ::
            StructField("profile.activity_first_time", LongType, true) ::
            Nil
        ), true) ::
        Nil
    )
    val org_features = spark.read.schema(ae_schema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT")
    org_features.createOrReplaceTempView("org_features")

    spark.sql(
      """
        |select
        |a.requestId,
        |b.organization,
        |b.tokenId,
        |b.type as `data.type`,
        |b.data.`channel` as `data.channel`,
        |b.data.`role` as `data.role`,
        |b.data.`sex` as `data.sex`,
        |b.features.`text.is_all_digit`,
        |b.features.`text.char_num`,
        |b.features.`text.normalized_char_num`,
        |b.features.`text.ch_char_num`,
        |b.features.`text.en_char_num`,
        |b.features.`text.digit_char_num`,
        |b.features.`text.unknown_char_num`,
        |b.features.`text.dup_char_num`,
        |b.features.`text.chinese_phrase_num`,
        |b.features.`ngram.text.score`,
        |b.features.`ngram.chtext.score`,
        |b.features.`advertise.score`,
        |b.features.`advertise.nickname.score`,
        |b.features.`contact.withNicknameResult.has_contact`,
        |b.features.`contact.withNicknameResult.wechat.DEFINITENESS`,
        |b.features.`contact.withNicknameResult.wechat.DISTANCE`,
        |b.features.`contact.withNicknameResult.qq.DISTANCE`,
        |b.features.`contact.textResult.has_contact`,
        |b.features.`contact.textResult.wechat.DEFINITENESS`,
        |b.features.`contact.textResult.wechat.DISTANCE`,
        |b.features.`contact.textResult.qq.DISTANCE`,
        |b.features.`redis-counter.org_text_count_per_10s` as `redis_counter.org_text_count_per_10s`,
        |b.features.`redis-counter.org_text_count_per_5m` as `redis_counter.org_text_count_per_5m`,
        |b.features.`redis-counter.org_text_count_per_30m` as `redis_counter.org_text_count_per_30m`,
        |b.features.`redis-counter.org_chtext_count_per_10s` as `redis_counter.org_chtext_count_per_10s`,
        |b.features.`redis-counter.org_chtext_count_per_5m` as `redis_counter.org_chtext_count_per_5m`,
        |b.features.`redis-counter.org_chtext_count_per_30m` as `redis_counter.org_chtext_count_per_30m`,
        |b.features.`redis-counter.text_simhash_per_org_token_20c_10s` as `redis_counter.text_simhash_per_org_token_20c_10s`,
        |b.features.`redis-counter.text_simhash_per_org_token_100c_5m` as `redis_counter.text_simhash_per_org_token_100c_5m`,
        |b.features.`redis-counter.text_simhash_per_org_token_100c_30m` as `redis_counter.text_simhash_per_org_token_100c_30m`,
        |b.features.`redis-counter.org_textwechat_count_per_10s` as `redis_counter.org_textwechat_count_per_10s`,
        |b.features.`redis-counter.org_textwechat_count_per_5m` as `redis_counter.org_textwechat_count_per_5m`,
        |b.features.`redis-counter.org_textwechat_count_per_30m` as `redis_counter.org_textwechat_count_per_30m`,
        |b.features.`redis-counter.token_dupcount_textwechat_30m` as `redis_counter.token_dupcount_textwechat_30m`,
        |b.features.`redis-counter.text_simhash_per_ip_100c_30m` as `redis_counter.text_simhash_per_ip_100c_30m`,
        |b.features.`redis-counter.ip_content_length_per_10s` as `redis_counter.ip_content_length_per_10s`,
        |b.features.`redis-counter.ip_content_length_per_5m` as `redis_counter.ip_content_length_per_5m`,
        |b.features.`redis-counter.ip_content_length_per_30m` as `redis_counter.ip_content_length_per_30m`,
        |b.features.`redis-counter.ip_count_per_10s` as `redis_counter.ip_count_per_10s`,
        |b.features.`redis-counter.ip_count_per_10m` as `redis_counter.ip_count_per_10m`,
        |b.features.`redis-counter.distinct_new_token_curtime_per_ip_3h` as `redis_counter.distinct_new_token_curtime_per_ip_3h`,
        |b.features.`redis-counter.event_cnt_per_token_tailsim_3h` as `redis_counter.event_cnt_per_token_tailsim_3h`,
        |b.features.`redis-counter.event_cnt_per_token_midsim_3h` as `redis_counter.event_cnt_per_token_midsim_3h`,
        |b.features.`redis-counter.event_cnt_per_token_headsim_3h` as `redis_counter.event_cnt_per_token_headsim_3h`,
        |b.features.`redis-counter.newtoken_cnt_per_txt_1d` as `redis_counter.newtoken_cnt_per_txt_1d`,
        |b.features.`redis-counter.newtoken_cnt_per_txt_3h` as `redis_counter.newtoken_cnt_per_txt_3h`,
        |b.features.`redis-counter.dup_cnt_per_newtoken_txt_3h` as `redis_counter.dup_cnt_per_newtoken_txt_3h`,
        |b.features.`redis-counter.dup_cnt_per_newtoken_txt_1d` as `redis_counter.dup_cnt_per_newtoken_txt_1d`,
        |b.features.`redis-counter.event_cnt_per_token_receiveToken_1d` as `redis_counter.event_cnt_per_token_receiveToken_1d`,
        |b.features.`redis-counter.distinct_text_per_token_receiveToken_1d` as `redis_counter.distinct_text_per_token_receiveToken_1d`,
        |b.features.`redis-counter.token_text_dup_per_30m` as `redis_counter.token_text_dup_per_30m`,
        |b.features.`redis-counter.distinct_text_per_token_receiveToken_30m` as `redis_counter.distinct_text_per_token_receiveToken_30m`,
        |b.features.`redis-counter.chtext_count_per_token_30m` as `redis_counter.chtext_count_per_token_30m`,
        |b.features.`redis-counter.distinct_text_per_token_30m` as `redis_counter.distinct_text_per_token_30m`,
        |b.features.`redis-counter.token_count_per_10s` as `redis_counter.token_count_per_10s`,
        |b.features.`redis-counter.token_count_per_1d` as `redis_counter.token_count_per_1d`,
        |b.features.`profile.activity_first_time` as `profile.activity_first_time`,
        |a.label
        |from org_features b
        |join allSamples a
        |on a.requestId = b.requestId
      """.stripMargin).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/org_features/$date")
  }

  /**
    * 检查并处理样本数据，reqeustId, org, token, label
    *
    * @param inputPath 样本输入路径
    * @param mark      黑白样本标记，1=黑样本，0=白样本
    */
  def checkAndProcessSample(inputPath: String, mark: Int): Unit = {
    val resultPath = if (1 == mark) path + "样本抽取\\blackSampleProcess.csv" else path + "样本抽取\\whiteSampleProcess.csv"

    val source = Source.fromFile(inputPath, "UTF-8")
    val lineIterator = source.getLines

    val out = new PrintWriter(resultPath)

    for (l <- lineIterator) {
      val info = l.split(",")
      if (1 == mark)
        out.println(info(0) + "," + info(1) + "," + info(2) + "," + "1")
      else
        out.println(info(0) + "," + info(1) + "," + info(2) + "," + "0")
    }

    out.close()
  }
}