package com.baixing.search.geli.Job

import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Digger, Data}
import org.apache.spark.SparkContext._

/**
 * Created by abzyme-baixing on 14-12-8.
 */
object DigAll {
	def main(args : Array[String]): Unit ={
		Env.init(args)

		val allTags = Data.allTag()

		val ad = Data.adContent().repartition(20)

		val query = Env.sparkContext().textFile("/user/sunyifan/ua/" + Env)
							.filter{row => row.split(",").length == 3}
								.map{row => row.split(",")(1)}
									.filter{q : String => q.length != 0 && q != "null"}
										.repartition(20)

		val seo = Env.sparkContext().textFile("/user/sunyifan/seo/" + Env)
							.filter{row => row.split(",").length == 3}
								.map{row => row.split(",")(1)}
									.filter{q : String => q.length != 0 && q != "null"}
										.repartition(20)

		val all = ad.union(query).union(seo).filter{item => allTags.toSet.contains(item)}


		val len = Digger.textLength(all)
		val text = Digger.processedText(all)
		val words = Digger.words(text)

		val freq = Digger.frequency(words, len)
		val consol = Digger.consolidate(freq)
		val free = Digger.freedom(words)

		freq.join(consol).join(free).map{
			item : (String, ((Double, Double), Double))
			=> (item._1, (item._2._1._1, item._2._1._2, item._2._2))
		}.sortByKey().saveAsTextFile("/user/sunyifan/geli/all/" + Env)

	}
}
