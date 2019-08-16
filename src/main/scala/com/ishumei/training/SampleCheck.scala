package com.ishumei.training

import scala.collection.mutable
import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 训练集与测试集的验证验证
  *       1. 测试集和训练集中的token不能重复，否则训练会报错退出
  *       2. 同一个token可以重复出现，但是label必须保持一致（即黑样本token与白样本不重复）
  */
object SampleCheck {
  val orgPath = "D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\样本抽取"

  def main(args: Array[String]): Unit = {
    //processSample()

    //checkBlackAndWhite

    checkTrainAndTest()
  }

  def checkTrainAndTest(): Unit = {
    val blackSet = mutable.Set[String]()
    val whiteSet = mutable.Set[String]()

    val blackSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\trainSet", "UTF-8")
    blackSource.getLines.foreach(l => {

      blackSet.add(l.split("\001")(0) + l.split("\001")(1))
    })

    val whiteSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\testSet", "UTF-8")
    whiteSource.getLines.foreach(l => whiteSet.add(l.split("\001")(0) + l.split("\001")(1)))

    val set = blackSet & whiteSet
    println(set.size)
    set.foreach(s => println(s))
  }

  def checkBlackAndWhite(): Unit = {
    val blackSet = mutable.Set[String]()
    val whiteSet = mutable.Set[String]()

    val blackSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\样本抽取\\blackSampleReq.csv", "UTF-8")
    blackSource.getLines.foreach(l => blackSet.add(l.split(",")(1) + l.split(",")(2)))
    val whiteSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\样本抽取\\whiteSampleReq.csv", "UTF-8")
    whiteSource.getLines.foreach(l => whiteSet.add(l.split(",")(1) + l.split(",")(2)))

    val set = blackSet & whiteSet
    println(set.size)
    set.foreach(s => println(s))
  }

  def processSample(): Unit = {
    val set = mutable.Set[String]()

    val source = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\trainSet", "UTF-8")
    val lineIterator = source.getLines

    var black = 0

    for (l <- lineIterator) {
      if (l.split("\001")(3) == "1")
        black += 1

    }
    println(black)
  }
}