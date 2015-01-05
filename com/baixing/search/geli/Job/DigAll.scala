package com.baixing.search.geli.Job

import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Rule, Digger, Data}
import org.apache.spark.SparkContext._

/**
 * Created by abzyme-baixing on 14-12-8.
 */
object DigAll {
	def main(args : Array[String]): Unit ={
		Env.init(args)

		val fangTag = Data.fangTag().collect()

		val ad = Data.adContent().repartition(Env.getProperty("partition").toInt)



		val len = Digger.textLength(ad)
		val text = Digger.processedText(ad)
		val words = Digger.words(text)

		val freq = Digger.frequency(words, len)
		val consol = Digger.consolidate(freq)
		val free = Digger.freedom(words)

		freq.join(consol).join(free).map{
			item : (String, ((Double, Double), Double))
			=> (item._1, (item._2._1._1, item._2._1._2, item._2._2))
		}.filter(item => Rule.containChinese(item._1))
			.filter(item => !Rule.containPearl(item._1, fangTag))
				.filter(item => !Rule.isPearl(item._1, fangTag.toSet))
				// .filter(item => Rule.aboveFreqThres(item._1, item._2._1))
					.filter(item => Rule.aboveConsolThres(item._1, item._2._2))
						.filter(item => Rule.aboveFreeThres(item._1, item._2._3))
							.map(item => item._1 + "," + item._2._1 + "," + item._2._2 + "," + item._2._3)
								.saveAsTextFile("/user/tianxing/geli/all/" + Env)
	}
}
