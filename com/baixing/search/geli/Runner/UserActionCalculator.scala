package com.baixing.search.geli.Runner

import com.baixing.search.geli.Configuration.Configuration
import com.baixing.search.geli.Digger.ThresholdDigger
import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.Data
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-19.
 */
object UserActionCalculator {
	def pearlUV(conf : Configuration, env : Env): RDD[(String, Int)] = {
		val ad2Visitor = Data.ad2VisitorInputRDD(conf, env)
		val adTags = Data.adTagInputRDD(conf, env)
		val pearlUV = ad2Visitor.join(adTags).flatMap{
			item : (String, (String, String)) =>
				val words = item._2._2.split("@")
				val ret = new ArrayBuffer[(String, String)]()

				for (word <- words){
					ret += ((word, item._2._2))
				}

				ret
		}.groupByKey().map(item => (item._1, item._2.toArray.distinct.length))

		pearlUV
	}

	def geli2pearlUV(conf : Configuration, env : Env) : RDD[((String,String), Int)] = {
		val adTags = Data.adTagInputRDD(conf, env)
		val allTags = Data.allTag(conf, env)

		val query = Data.UserActionInputRDD(conf, env)
		val geliInQuery = ThresholdDigger.dig(query)
											.filter{item : String =>
												for (tag <- allTags) {
													if (tag.toLowerCase == item.toLowerCase)
														false
												}

												true
											}


		val ad2geli = query.map{
			item : (String, String) =>
				val visitor_id = item._1.split("\\|")(0)
				val ad_id = item._1.split("\\|")(1)
				val query = item._2

				var ret = new ArrayBuffer[String]

				for (word <- geliInQuery){
					if (query.indexOf(word) >= 0){
						ret += word
					}
				}

				(ad_id, (visitor_id, ret.toArray))
		}



		val ads = Data.adInput(conf, env)
		val ad2pearl = ads.join(adTags).map{
			item : (String, (String, String))
			=> (item._1, item._2._2)
		}

		val geli2pearlUV = ad2geli.join(ad2pearl).flatMap{
			item : (String, ((String,Array[String]), String)) =>
				val pearls = item._2._2.split("@")
				val gelis = item._2._1._2
				val visitor_id = item._2._1._1

				var ret = new ArrayBuffer[((String, String), String)]

				for(pearl <- pearls){
					for(geli <- gelis){
						ret += (((pearl,geli),visitor_id))
					}
				}

				ret
		}.groupByKey().map{
			item : ((String, String), Iterable[String]) =>
				(item._1, item._2.toArray.length)
		}

		geli2pearlUV
	}
}
