package com.baixing.search.geli.Runner

import com.baixing.search.geli.Configuration.Configuration
import com.baixing.search.geli.Digger.ThresholdDigger
import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.Data
import com.baixing.search.geli.Word.WordAttributeBuilder
import org.apache.spark.SparkContext._

/**
 * Created by abzyme-baixing on 14-11-13.
 */
object WordRelationRunner {
	def main(args : Array[String]): Unit ={
		val conf = new Configuration(args(0), args(1), args(2), args(3))
		val env = new Env(conf)

		val ads = Data.AdInput(conf, env)
		val pearlWordsAttribute = WordAttributeBuilder.pearl2Ad(Data.adTagInputRDD(conf, env))

		val geliWords = ThresholdDigger.dig(ads).toSet.diff(pearlWordsAttribute.keys.collect.toSet).toArray
		val geliWordAttribute = WordAttributeBuilder.word2Ad(ads, geliWords)

		val adNum = ads.count()
		val wordRelations = WordAttributeBuilder.wordRelations(geliWordAttribute, pearlWordsAttribute)

		println(wordRelations.count())
		// wordRelations.saveAsTextFile(args(5))
	}
}
