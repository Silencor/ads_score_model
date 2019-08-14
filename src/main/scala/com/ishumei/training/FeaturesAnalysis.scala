package com.ishumei.training

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 特征缺失率分析
  */
object FeaturesAnalysis {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\testSet", "UTF-8")
    val lineIterator = source.getLines

    val set = Set[String]()

    for (l <- lineIterator) {
      val features = l.split("\001")(2)

      set.add(features.split("\t")(23))
    }

    println(set.size)
    set.foreach(s => println(s))
  }
}