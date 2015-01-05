package com.baixing.search.geli.Job

import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Data, Text}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-12-8.
 */
object RelationByQuery {
		def geliUV(gelis : Array[String], uaWithQuery : RDD[(String, String)]): RDD[(String, Array[String])] ={
			uaWithQuery.flatMap{
				item : (String, String) =>
					val visitor = item._1
					val query = item._2
					val ret = new ArrayBuffer[(String, String)]

					for (geli <- gelis){
						if (query.indexOf(geli) >= 0)
							ret += ((geli, visitor))
					}

					ret
			}.groupByKey().map{
				item =>
					(item._1, item._2.toArray)
			}
		}

		def pearlUV(adTag : RDD[(String, String)], uaWithAd : RDD[(String, String)]): RDD[(String, Array[String])] = {
			adTag.join(uaWithAd.map{item => (item._2, item._1)})
				.flatMap{
				item : (String, (String, String)) =>
					val visitor = item._2._2
					val tags = item._2._1.split("@")
					val ret = new ArrayBuffer[(String, String)]()

					for(tag <- tags){
						ret += ((tag, visitor))
					}

					ret
			}.groupByKey().map{
				item =>
					(item._1, item._2.toArray)
			}
		}


		def main(args : Array[String]): Unit ={
			Env.init(args)

			val fangTag = Data.fangTag()
			val allTag = Data.allTag()

			val adTag = Env.sparkContext().textFile("/user/sunyifan/adTag/" + Env).filter(_.length > 0).map{
				line : String =>
					val item = line.substring(1, line.length - 1).split(",")
					(item.head.replace("(", ""), item.last.replace(")", ""))
			}.repartition(20)

			val rawUserAction = Env.sparkContext().textFile("/user/sunyifan/ua/" + Env).map{
				line : String =>
					val item = line.split(",")
					(item.head.replace("[", ""),
						item.slice(1, item.length - 1).mkString(","),
						item.last.replace("]",""))
			}.repartition(20)

			val uaWithAd = rawUserAction.filter{
				item => item._3 != "0"
			}.map{
				item => (item._1, item._3)
			}

			val uaWithQueryAndAd = rawUserAction.filter{
				item => item._2.indexOf("query=") >= 0 && item._3 != "0"
			}.map{
				item =>
					val q = item._2.split(",").filter(_.startsWith("query="))(0).substring(6)
					(item._1, q, item._3)
			}


			val uaWithQuery = rawUserAction.filter{
				item => item._2.indexOf("query=") >= 0 && item._2.split(",").filter(_.startsWith("query=")).length > 0
			}.map{
				item =>
					val q = item._2.split(",").filter(_.startsWith("query="))(0).substring(6)
					(item._1, q)
			}

			val rawSeo = Env.sparkContext().textFile("/user/sunyifan/seo/" + Env).filter{
				line => line.split(",").length == 3
			}.map{
				line : String =>
					val item = line.split(",")
					(item(0).replace("[", ""), item(1), item(2).replace("]",""))
			}.repartition(20)


			val seoWithAd = rawSeo.filter{
				item => item._3 != "0"
			}.map{
				item => (item._1, item._3)
			}

			val seoWithQueryAndAd = rawSeo.filter{
				item => item._2 != "null" && item._2.length > 0 && item._3 != "0"
			}

			val seoWithQuery = rawSeo.filter{
				item => item._2 != "null" && item._2.length > 0
			}.map{
				item =>
					(item._1, item._2)
			}


			// 上面是临时拿数据的地方
			val totalUV = rawUserAction.union(rawSeo).map(_._1).distinct().count()
			val pearl2UV = pearlUV(adTag, uaWithAd.union(seoWithAd))


			val gelis = Env.sparkContext().textFile("/user/sunyifan/geli/all/" + Env).map{line => line.split(",").head}
			val bGelis = Env.sparkContext().broadcast(gelis.collect())

			val geli2UV = geliUV(bGelis.value, uaWithQuery.union(seoWithQuery))
			geli2UV.take(100).foreach(println)

			val result = pearl2UV.cartesian(geli2UV).map{
				item : ((String, Array[String]), (String, Array[String])) =>
					val pearl = item._1._1
					val pearlVisitor = item._1._2
					val geli = item._2._1
					val geliVisitor = item._2._2

					val pearl2geliUV = pearlVisitor.intersect(geliVisitor).distinct.length
					val pearlUV = pearlVisitor.distinct.length
					val geliUV = geliVisitor.distinct.length

					(
						geli,
						pearl,
						pearl2geliUV.toDouble / totalUV,
						(pearl2geliUV.toDouble / geliUV.toDouble) * (totalUV.toDouble / pearlUV.toDouble),
						(pearl2geliUV.toDouble / geliUV.toDouble)
						)
			}.filter(_._4 > 1).map{
				item  =>
					(item._1, (item._2, item._3, item._4, item._5))
			}.groupByKey().flatMap{
				item : (String, Iterable[(String, Double, Double, Double)]) =>
					val sortedArray = item._2.toArray.sortWith(_._4 > _._4)
					sortedArray.map{ elem : (String, Double, Double, Double) => (item._1, elem._1, elem._2, elem._3, elem._4)}
			}

			result.saveAsTextFile("/user/sunyifan/relation/all/" + Env + "-query")
		}
}
