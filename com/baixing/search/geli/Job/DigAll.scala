package com.baixing.search.geli.Job

import com.baixing.search.geli.Digger.ThresholdDigger
import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Data, Text}

/**
 * Created by abzyme-baixing on 14-12-8.
 */
object DigAll {
	def main(args : Array[String]): Unit ={
		Env.init(args)

		val allTags = Data.allTag()

		val ad = Data.adContent().repartition(20)

		val query = Env.sparkContext().textFile("/user/sunyifan/ua/" + Env.output())
							.filter{row => row.split(",").length == 3}
								.map{row => row.split(",")(1)}
									.filter{q : String => q.length != 0 && q != "null"}
										.repartition(20)

		val seo = Env.sparkContext().textFile("/user/sunyifan/seo/" + Env.output())
							.filter{row => row.split(",").length == 3}
								.map{row => row.split(",")(1)}
									.filter{q : String => q.length != 0 && q != "null"}
										.repartition(20)

		val all = ad.union(query).union(seo).filter{item => Text.find(allTags, item) < 0}
		ThresholdDigger.dig(all).saveAsTextFile("/user/sunyifan/geli/" + Env.src() + "/" + Env.output())
	}
}
