package com.baixing.search.geli.Word

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object WordAttributeBuilder {
	def word2Ad(inputRDD : RDD[(String, String)], wordList : Array[String]): RDD[(String, String)] = {
		inputRDD.flatMap{
			item : (String, String)=>

				val res = new ArrayBuffer[(String, String)]()
				for (word <- wordList){
					if (item._2.indexOf(word) >= 0){
						res += ((word, item._1))
					}
				}

				res.distinct
		}.reduceByKey(_ + "|" + _)
	}

	def wordRelations(wordRDD1 : RDD[(String, String)], wordRDD2: RDD[(String, String)]): RDD[((String, String), Double)] ={
		wordRDD1.cartesian(wordRDD2).map{
			line : ((String, String), (String, String)) =>
				val geli = line._1._1
				val chuanzhu = line._2._1
				val geliAdIds = line._1._2.split("\\|")
				val chuanzhuAdIds = line._2._2.split("\\|")

				val commonIds = geliAdIds ++ chuanzhuAdIds

				((geli, chuanzhu),
					(geliAdIds.length + chuanzhuAdIds.length - commonIds.distinct.length) / geliAdIds.length.toDouble)
		}
	}
}
