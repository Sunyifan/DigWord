package com.baixing.search.geli.Word

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object WordAttributeBuilder {
	def pearl2Ad(adTags : RDD[(String, String)], ads:RDD[(String, String)]): RDD[(String, Array[String])] ={

		ads.join(adTags).map{
			item : (String, (String, String))
			=> (item._1, item._2._2)
		}.flatMap{
			item =>
				val ret = new ArrayBuffer[(String, String)]

				for (word <- item._2.split("@")){
					ret += ((word, item._1))
				}

				ret
		}.groupByKey().map{item => (item._1, item._2.toArray)}
	}

	def word2Ad(inputRDD : RDD[(String, String)], wordList : Array[String]): RDD[(String, Array[String])] = {
		inputRDD.flatMap{
			item : (String, String)=>

				val res = new ArrayBuffer[(String, String)]()
				for (word <- wordList){
					if (item._2.indexOf(word) >= 0){
						res += ((word, item._1))
					}
				}

				res.distinct
		}.groupByKey().map{item => (item._1, item._2.toArray)}
	}

	def word2Query(queryRDD : RDD[(String, String)], wordList : Array[String]): Unit = {
		queryRDD.map{
			item : (String, String) =>
				val res = new ArrayBuffer[(String, String)]()
				for (word <- wordList){
					if (item._2.indexOf(word) >= 0){
						res += ((item._1, item._1))
					}
				}
		}
	}

	def wordRelations(wordRDD1 : RDD[(String, Array[String])], wordRDD2: RDD[(String, Array[String])]): RDD[((String, String), Double)] ={
		wordRDD2.cartesian(wordRDD1).map{
			line  =>
				val chuanzhu = line._1._1
				val geli = line._2._1
				val chuanzhuAdIds = line._1._2
				val geliAdIds = line._2._2

				val commonIds = geliAdIds ++ chuanzhuAdIds

				((chuanzhu, geli),
					(geliAdIds.length + chuanzhuAdIds.length - commonIds.distinct.length) / geliAdIds.length.toDouble)
		}.filter{item : ((String, String), Double) => item._2 > 0.05 && item._2 < 1 }
	}
}
