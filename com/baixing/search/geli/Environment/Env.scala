package com.baixing.search.geli.Environment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Env {
	private var sc : SparkContext = null
	private var conf : SparkConf = null

	def sparkContext() : SparkContext = {
		if (sc == null)
			sc = new SparkContext(conf)
		sc
	}

	def sparkConf() : SparkConf = {
		if (conf == null)
			conf = new SparkConf()
		conf
	}

	def hiveContext() : HiveContext = {
		new HiveContext(sc)
	}

	def init(): Unit = {
		sparkConf()
		sparkContext()
	}

	def digEnv(args : Array[String]) : Unit = {
		sparkConf().set("area_id", args(0))
		sparkConf().set("category", args(1))
		sparkConf().set("fromdate", args(2))
		sparkConf().set("todate", args(3))
	}

	override def toString() : String = {
		sparkConf().get("area_id") + "-" + sparkConf().get("category") + sparkConf().get("fromdate") + "-" + sparkConf().get("todate")
	}
}

