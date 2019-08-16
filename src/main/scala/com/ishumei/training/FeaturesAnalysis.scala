package com.ishumei.training

import scala.collection.mutable
import scala.io.Source

/**
  * Copyright (c) 2015 Shumei Inc. All Rights Reserved.
  * Authors: niujian <niujian@ishumei.com>
  * TODO: 特征缺失率分析
  */
object FeaturesAnalysis {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\SM\\数美科技-数据挖掘\\广告导流\\ads_score_v0.2\\特征关联\\trainSet", "UTF-8")
    val lineIterator = source.getLines

    val set = mutable.Set[String]()

    var black = 0
    var white = 0

    for (l <- lineIterator) {
      val info = l.split("\001")

      val features = info(2)
      val label = info(3)

      if ("0" == label) black += 1 else white += 1

      set.add(features.split("\t")(23))
    }

    println(black)

  }
}