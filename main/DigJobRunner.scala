package main

import config.Configuration
import digger.ThresholdDigger
import environ.Env
import util.DataStorage
import word.WordAttrBuilder

/**
 * Created by abzyme-baixing on 14-11-11.
 */
object DigJobRunner {
	// args: cityid, category, fromdate, todate
	def main(args : Array[String]): Unit ={
		// 初始化任务配置
		val conf = new Configuration(args(0), args(1), args(2), args(3))
		val env = new Env(conf)

		// 获得数据
		val inputRDD = DataStorage.generateInputRDD(env, conf)

		// 挖词
		val wordList = ThresholdDigger.dig(inputRDD)

		// 词属性
		val wordInAdCnt = WordAttrBuilder.wordInAdCount(inputRDD, wordList).take(5).foreach(println)
	}
}
