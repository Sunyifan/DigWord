package com.baixing.search.geli.Job

import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Data, Text}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by abzyme-baixing on 14-11-26.
  */
object RelationByAd {
	 def pearlAd(ads : RDD[(String, String, String)]): RDD[(String, Int)] ={
		 ads.flatMap {
			 item: (String, String, String) =>
				 val pearls = item._3.split("@")
				 val ad_id = item._1
				 val ret = new ArrayBuffer[(String, String)]()

				 for (pearl <- pearls)
					 if (item._2.indexOf(pearl) >= 0)
					    ret += ((pearl, ad_id))

				 ret
		 }.groupByKey().map {
			 item: (String, Iterable[String]) =>
				 (item._1, item._2.toArray.distinct.length)
		 }.filter(_._2 != 0)
	 }

	 def geliAd(ads : RDD[(String, String, String)], gelis : Array[String]): RDD[(String, Int)] ={
		 ads.flatMap {
			 item: (String, String, String) =>
				 val ad_id = item._1
				 val ret = new ArrayBuffer[(String, String)]()

				 for (geli <- gelis)
					 if (item._2.indexOf(geli) >= 0)
						 ret += ((geli, ad_id))

				 ret
		 }.groupByKey().map {
			 item: (String, Iterable[String]) =>
				 (item._1, item._2.toArray.distinct.length)
		 }
	 }


	 def pearlAndGeli(ads : RDD[(String, String, String)], gelis : Array[String]): RDD[((String, String), Int)] = {
		 ads.flatMap{
			 item =>
				 val ret = new ArrayBuffer[String]()
				 val geliInAd = new ArrayBuffer[String]()
				 val pearlInAd = item._3.split("@")

				 for (geli <- gelis){
					 if (item._2.indexOf(geli) >= 0)
						 geliInAd += geli
				 }

				 for(geli <- geliInAd; pearl <- pearlInAd){
					 if (item._2.indexOf(pearl) >= 0 && item._2.indexOf(geli) >= 0)
						 ret += pearl + "@" + geli
				 }

				 ret.distinct
		 }.map((_, 1)).reduceByKey(_ + _).map{
			 item : (String, Int) =>
				 val t = item._1.split("@")
				 ((t(0), t(1)), item._2)
		 }
	 }

	 def geli(allTag : Array[String]): Array[String] ={
		 Data.gelis().map {
			 line: String =>
				 line.split(",")(0).substring(1)
		 }.filter{ geli: String => allTag.toSet.contains(geli)}.collect
	 }

	 def main(args : Array[String]): Unit = {
		 Env.init(args)

		 val fangTag = Data.fangTag()
		 val allTag = Data.allTag()
		 val gelis = geli(allTag)

		 val ads = Data.adContentWithId()
		 val adNum = ads.count()

		 val adContentWithIdAndTag = Data.adContentWithIdAndTag()

		 val pearl2Ad = pearlAd(adContentWithIdAndTag).filter{item => fangTag.toSet.contains(item._1)}
		 val geli2Ad = geliAd(adContentWithIdAndTag, gelis).filter{item => allTag.toSet.contains(item._1)}
		 val pearl2Geli = pearlAndGeli(adContentWithIdAndTag, gelis)


		 val result = pearl2Ad.join(pearl2Geli.map{
			 item : ((String, String), Int) =>
				 (item._1._1, (item._1._2, item._2))
		 }).map{
			 item : (String, (Int, (String, Int))) =>
				 (item._2._2._1, (item._1, item._2._1, item._2._2._2))
		 }.join(geli2Ad)
			 .map{
				 item : (String, ((String, Int, Int), Int)) =>
					 (item._1, item._2._1._1, item._2._1._2, item._2._1._3, item._2._2)
			 }.map{
				 item =>
					 val geli = item._1
					 val pearl = item._2
					 val pearlCnt = item._3
					 val pearl2GeliCnt = item._4
					 val geliCnt = item._5

					 (
						 geli,
						 pearl,
						 pearl2GeliCnt.toDouble / adNum,
						 (pearl2GeliCnt.toDouble / geliCnt) * (adNum.toDouble / pearlCnt),
						 pearl2GeliCnt.toDouble / geliCnt
					 )
			 }.map{
				 item  =>
					 (item._1, (item._2, item._3, item._4, item._5))
			 }.filter(_._2._3 > 1).groupByKey()
				 .flatMap{
					 item : (String, Iterable[(String, Double, Double, Double)]) =>
					 val sortedArray = item._2.toArray.sortWith(_._4 > _._4)
					 sortedArray.map{ elem : (String, Double, Double, Double) => (item._1, elem._1, elem._2, elem._3, elem._4)}
				 }

		 result.saveAsTextFile("/user/sunyifan/relation/all/" + Env.output() + "-ad")
	 }
 }
