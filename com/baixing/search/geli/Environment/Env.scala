package com.baixing.search.geli.Environment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Env {
	private var sc : SparkContext = null
	private val conf : SparkConf = new SparkConf()

	def sparkContext() : SparkContext = {
		if (sc == null)
			sc = new SparkContext(conf)
		sc
	}

	def hiveContext() : HiveContext = {
		new HiveContext(sc)
	}

	private def set(k : String, v : String) = conf.set(k, v)

	def getProperty(k : String) : String = {
		conf.get(k)
	}

	def init(args : Array[String]): Unit = {
		set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		set("area_id", args(0))
		set("category", args(1))
		set("fromdate", args(2))
		set("todate", args(3))
		set("type", args(4))
		sparkContext()
	}

	private val ROOT = "/user/sunyifan"

	def job() : String = {
		getProperty("type").split("\\.")(0)
	}

	def src() : String = {
		getProperty("type").split("\\.")(1)
	}

	def output() : String= {
		getProperty("area_id") + "-" + getProperty("category") + "-" + getProperty("fromdate") + "-" + getProperty("todate")
	}
}

