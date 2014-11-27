package com.baixing.search.geli.Environment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Env {
	private var sc : SparkContext = null
	private var hc : HiveContext = null
	private val conf : SparkConf = new SparkConf()

	def sparkContext() : SparkContext = {
		if (sc == null)
			sc = new SparkContext(conf)
		sc
	}

	def hiveContext() : HiveContext = {
		if (hc == null)
			hc = new HiveContext(sc)
		hc
	}

	private def set(k : String, v : String) = conf.set(k, v)
	def setMaster(master : String) = conf.setMaster(master)
	def setAppName(appName : String) = conf.setAppName(appName)
	def setJar(jar : String) = conf.setJars(Array(jar))
	def setExecutorMemoery(mem : String) = conf.set("spark.executor.memory", mem)

	def getProperty(k : String) : String = {
		conf.get(k)
	}

	def init(args : Array[String]): Unit = {
		set("area_id", args(0))
		set("category", args(1))
		set("fromdate", args(2))
		set("todate", args(3))
		setAppName(args(4))
		setMaster(args(5))
		setJar(args(6))
		setExecutorMemoery(args(7))
		sparkContext()
	}
}

