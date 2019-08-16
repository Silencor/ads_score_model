package com.ishumei.evaluating

import com.ishumei.util.SparkUtils

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 获取一天的文本事件特征
  */
object GetOneDayTextFeatures {
  def getFeatures(): Unit ={
    val spark = SparkUtils.getSparkSession("GetOneDayTextFeatures")

    import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
    val request_features_schema = StructType(
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
        StructField("requestId", StringType, true) ::
        Nil
    )

    val date = "dt=20190815"
    val request_features_1d = spark.read.schema(request_features_schema).json(s"/user/data/event/detail_ae/$date/serviceId=POST_TEXT/")
    request_features_1d.createOrReplaceTempView("table_request_features")

    spark.sql(
      """
        |select
        |organization,
        |tokenId,
        |type as `data.type`,
        |data.`channel` as `data.channel`,
        |data.`role` as `data.role`,
        |data.`sex` as `data.sex`,
        |features.`text.is_all_digit`,
        |features.`text.char_num`,
        |features.`text.normalized_char_num`,
        |features.`text.ch_char_num`,
        |features.`text.en_char_num`,
        |features.`text.digit_char_num`,
        |features.`text.unknown_char_num`,
        |features.`text.dup_char_num`,
        |features.`text.chinese_phrase_num`,
        |features.`ngram.text.score`,
        |features.`ngram.chtext.score`,
        |features.`advertise.score`,
        |features.`advertise.nickname.score`,
        |features.`contact.withNicknameResult.has_contact`,
        |features.`contact.withNicknameResult.wechat.DEFINITENESS`,
        |features.`contact.withNicknameResult.wechat.DISTANCE`,
        |features.`contact.withNicknameResult.qq.DISTANCE`,
        |features.`contact.textResult.has_contact`,
        |features.`contact.textResult.wechat.DEFINITENESS`,
        |features.`contact.textResult.wechat.DISTANCE`,
        |features.`contact.textResult.qq.DISTANCE`,
        |features.`redis-counter.org_text_count_per_10s` as `redis_counter.org_text_count_per_10s`,
        |features.`redis-counter.org_text_count_per_5m` as `redis_counter.org_text_count_per_5m`,
        |features.`redis-counter.org_text_count_per_30m` as `redis_counter.org_text_count_per_30m`,
        |features.`redis-counter.org_chtext_count_per_10s` as `redis_counter.org_chtext_count_per_10s`,
        |features.`redis-counter.org_chtext_count_per_5m` as `redis_counter.org_chtext_count_per_5m`,
        |features.`redis-counter.org_chtext_count_per_30m` as `redis_counter.org_chtext_count_per_30m`,
        |features.`redis-counter.text_simhash_per_org_token_20c_10s` as `redis_counter.text_simhash_per_org_token_20c_10s`,
        |features.`redis-counter.text_simhash_per_org_token_100c_5m` as `redis_counter.text_simhash_per_org_token_100c_5m`,
        |features.`redis-counter.text_simhash_per_org_token_100c_30m` as `redis_counter.text_simhash_per_org_token_100c_30m`,
        |features.`redis-counter.org_textwechat_count_per_10s` as `redis_counter.org_textwechat_count_per_10s`,
        |features.`redis-counter.org_textwechat_count_per_5m` as `redis_counter.org_textwechat_count_per_5m`,
        |features.`redis-counter.org_textwechat_count_per_30m` as `redis_counter.org_textwechat_count_per_30m`,
        |features.`redis-counter.token_dupcount_textwechat_30m` as `redis_counter.token_dupcount_textwechat_30m`,
        |features.`redis-counter.text_simhash_per_ip_100c_30m` as `redis_counter.text_simhash_per_ip_100c_30m`,
        |features.`redis-counter.ip_content_length_per_10s` as `redis_counter.ip_content_length_per_10s`,
        |features.`redis-counter.ip_content_length_per_5m` as `redis_counter.ip_content_length_per_5m`,
        |features.`redis-counter.ip_content_length_per_30m` as `redis_counter.ip_content_length_per_30m`,
        |features.`redis-counter.ip_count_per_10s` as `redis_counter.ip_count_per_10s`,
        |features.`redis-counter.ip_count_per_10m` as `redis_counter.ip_count_per_10m`,
        |features.`redis-counter.distinct_new_token_curtime_per_ip_3h` as `redis_counter.distinct_new_token_curtime_per_ip_3h`,
        |features.`redis-counter.event_cnt_per_token_tailsim_3h` as `redis_counter.event_cnt_per_token_tailsim_3h`,
        |features.`redis-counter.event_cnt_per_token_midsim_3h` as `redis_counter.event_cnt_per_token_midsim_3h`,
        |features.`redis-counter.event_cnt_per_token_headsim_3h` as `redis_counter.event_cnt_per_token_headsim_3h`,
        |features.`redis-counter.newtoken_cnt_per_txt_1d` as `redis_counter.newtoken_cnt_per_txt_1d`,
        |features.`redis-counter.newtoken_cnt_per_txt_3h` as `redis_counter.newtoken_cnt_per_txt_3h`,
        |features.`redis-counter.dup_cnt_per_newtoken_txt_3h` as `redis_counter.dup_cnt_per_newtoken_txt_3h`,
        |features.`redis-counter.dup_cnt_per_newtoken_txt_1d` as `redis_counter.dup_cnt_per_newtoken_txt_1d`,
        |features.`redis-counter.event_cnt_per_token_receiveToken_1d` as `redis_counter.event_cnt_per_token_receiveToken_1d`,
        |features.`redis-counter.distinct_text_per_token_receiveToken_1d` as `redis_counter.distinct_text_per_token_receiveToken_1d`,
        |features.`redis-counter.token_text_dup_per_30m` as `redis_counter.token_text_dup_per_30m`,
        |features.`redis-counter.distinct_text_per_token_receiveToken_30m` as `redis_counter.distinct_text_per_token_receiveToken_30m`,
        |features.`redis-counter.chtext_count_per_token_30m` as `redis_counter.chtext_count_per_token_30m`,
        |features.`redis-counter.distinct_text_per_token_30m` as `redis_counter.distinct_text_per_token_30m`,
        |features.`redis-counter.token_count_per_10s` as `redis_counter.token_count_per_10s`,
        |features.`redis-counter.token_count_per_1d` as `redis_counter.token_count_per_1d`,
        |features.`profile.activity_first_time` as `profile.activity_first_time`,
        |requestId
        |from table_request_features
      """.stripMargin).repartition(100).write.csv(s"/user/data/tianwang/niujian/ads_score_v0.2/evaluation/org_features/$date")
  }

  def getAllFeatures(): Unit ={
    val spark = SparkUtils.getSparkSession("getAllFeatures")

    import java.util.Date
    val date = "dt=20190815"
    val orgFeatures = spark.sparkContext.textFile(s"/user/data/tianwang/niujian/ads_score_v0.2/evaluation/org_features/$date/")
    val allFeatures = orgFeatures.map(row => {
      val features = row.split(",")

      val line = new StringBuilder
      if (features.length == 64) {
        line.append(features(0) + "\001"+ features(1) + "\001")

        val data_type = features(2) // 类型，分成两类（ZHIBO表示直播，其他都是论坛FORUM)
        if ("ZHIBO" == data_type) line.append("01").append("\t")
        else if ("" == data_type) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_channel = features(3) // 渠道, 分成两类（MESSAGE/IM_TEXT表示私信，其它表示公开）
        if ("MESSAGE" == data_channel || "IM_TEXT" == data_channel) line.append("01").append("\t")
        else if ("" == data_channel) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_role = features(4) // 角色，分成两类（NORMAL、SPECIAL）
        if ("NORMAL" == data_role) line.append("01").append("\t")
        else if ("" == data_role) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val data_sex = features(5) // 性别
        if ("0" == data_sex) line.append("01").append("\t")
        else if ("" == data_sex) line.append("-1.0").append("\t")
        else line.append("10").append("\t")

        val text_is_all_digit = features(6) // 是否为纯数字
        if ("TRUE" == text_is_all_digit) line.append("0").append("\t")
        else if ("" == text_is_all_digit) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        for (i <- 7 to 18) {
          if ("" == features(i)) line.append("-1.0").append("\t")
          else line.append(features(i)).append("\t")
        }

        val contact_withNicknameResult_has_contact = features(19) // 昵称加文本是否包含联系方式
        if ("TRUE" == contact_withNicknameResult_has_contact) line.append("0").append("\t")
        else if ("" == contact_withNicknameResult_has_contact) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        var enCharacter = 0 // 字母个数
        var numberCharacter = 0 // 数字个数
        val contact_withNicknameResult_wechat_DEFINITENESS = features(20) // 昵称加文本中的联系方式串
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

        val contact_withNicknameResult_wechat_DISTANCE = features(21) // 昵称加文本中关键词和串的距离
        if ("" == contact_withNicknameResult_wechat_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_withNicknameResult_wechat_DISTANCE).append("\t")

        val contact_withNicknameResult_qq_DISTANCE = features(22) // 昵称加文本中QQ关键词距离串的距离
        if ("" == contact_withNicknameResult_qq_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_withNicknameResult_qq_DISTANCE).append("\t")

        val contact_textResult_has_contact = features(23) // 文本是否包含联系方式
        if ("TRUE" == contact_textResult_has_contact) line.append("0").append("\t")
        else if ("" == contact_textResult_has_contact) line.append("-1.0").append("\t")
        else line.append("1").append("\t")

        val contact_textResult_wechat_DEFINITENESS = features(24) // 文本中的联系方式串
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

        val contact_textResult_wechat_DISTANCE = features(25) // 文本中关键词和串的距离
        if ("" == contact_textResult_wechat_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_textResult_wechat_DISTANCE).append("\t")

        val contact_textResult_qq_DISTANCE = features(26) // 文本中QQ关键词距离串的距离
        if ("" == contact_textResult_qq_DISTANCE) line.append("-1.0").append("\t")
        else line.append(contact_textResult_qq_DISTANCE).append("\t")

        // redis-counter
        for (n <- 27 to 61) {
          if ("" == features(n)) line.append("0").append("\t")
          else line.append(features(n)).append("\t")
        }

        val profile_activity_first_time = features(62) // 账号首次活跃时间，计算到当前时间的时间差
        if ("" == profile_activity_first_time) line.append("-1.0")
        else { // 不为空，计算到当前的时间差
          val date = profile_activity_first_time.toLong
          val nowDate = new Date().getTime
          line.append(nowDate - date)
        }

        // label
        line.append("\001" + "-1")
        line.append("\001"+ features(63))
      }

      line.toString()
    }).filter(str => !str.equals(""))

    // org \001 token \001 feature0 \t feature1... \001 label \001 requestId
    allFeatures.saveAsTextFile(s"/user/data/tianwang/niujian/ads_score_v0.2/evaluation/process_features/$date")
  }
}