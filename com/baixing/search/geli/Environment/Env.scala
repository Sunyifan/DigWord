package com.baixing.search.geli.Environment

import com.baixing.search.geli.Configuration.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by abzyme-baixing on 14-11-12.
 */
class Env (conf : Configuration){
	val sparkContext : SparkContext = new SparkContext(new SparkConf().setAppName("digword_" + conf.toString()))
	def getSparkContext() : SparkContext = sparkContext
}
