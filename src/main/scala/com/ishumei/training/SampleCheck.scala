package com.ishumei.training

import scala.collection.mutable.Set
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
    //dropDupWhiteSample()

    // checkBlackAndWhite

    checkTrainAndTest()
  }

  def checkTrainAndTest() : Unit = {
    val blackSet = Set[String]()
    val whiteSet = Set[String]()

    val blackSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\trainSet", "UTF-8")
    blackSource.getLines.foreach(l => blackSet.add(l.split("\001")(0) + "," +  l.split("\001")(1)))
    val whiteSource = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\testSet", "UTF-8")
    whiteSource.getLines.foreach(l => whiteSet.add(l.split("\001")(0) + "," + l.split("\001")(1)))

    val set = blackSet & whiteSet
    println(set.size)
    set.foreach(s => println(s))
  }

  def checkBlackAndWhite() : Unit = {
    val blackSet = Set[String]()
    val whiteSet = Set[String]()

    val blackSource = Source.fromFile(orgPath + "\\blackSample.csv", "UTF-8")
    blackSource.getLines.foreach(l => blackSet.add(l.split(",")(1) + l.split(",")(2)))
    val whiteSource = Source.fromFile(orgPath + "\\whiteSample.csv", "UTF-8")
    whiteSource.getLines.foreach(l => whiteSet.add(l.split(",")(1) + l.split(",")(2)))

    val set = blackSet & whiteSet
    println(set.size)
    set.foreach(s => println(s))
  }

  /**
    * 样本去重
    */
  def dropDupWhiteSample() : Unit = {
    val set = Set[String]()

    val source = Source.fromFile(orgPath + "\\blackSample.csv", "UTF-8")
    val lineIterator = source.getLines

    for (l <- lineIterator) {
      set.add(l)
    }

    println(set.size)
    set.foreach(s => println(s))
  }
}