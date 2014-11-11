package main

import config.Configuration
import environ.Env
import util.DataStorage

/**
 * Created by abzyme-baixing on 14-11-11.
 */
object DigJobRunner {
	// args: cityid, category, fromdate, todate
	def main(args : Array[String]): Unit ={
		val conf = new Configuration(args(0), args(1), args(2), args(3))
		val env = new Env(conf)

		val inputRDD = DataStorage.generateInputRDD(env, conf)
		inputRDD.take(5).foreach(println)
	}
}
