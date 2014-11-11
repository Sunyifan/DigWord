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
		val area_id = args(0)
		val category = args(1)
		val fromdate = args(2)
		val todate = args(3)


		// 初始化任务配置
		val conf = new Configuration(area_id, category, fromdate, todate)
		val env = new Env(conf)

		// 获得数据
		val inputRDD = DataStorage.buildInputRDD(env, conf)

		// 挖词
		val wordList = ThresholdDigger.dig(inputRDD)

		// 词属性
		val wordInAdCnt = WordAttrBuilder.wordInAdCount(inputRDD, wordList)

		// todo: store result

	}
}
