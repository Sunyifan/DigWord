package com.baixing.search.geli.Digger

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import com.baixing.search.geli.Util.Text

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object ThresholdDigger {
	def textLength(text : RDD[String]) : Long = text.map(_.length).reduce(_ + _)
	def processedText(text : RDD[String]) : RDD[String] = text.flatMap{item : String => Text.preproccess(item)}
	def words(text : RDD[String], maxWordLength : Int = 10) : RDD[String] = {
		text.flatMap{line : String => Text.splitWord(line, maxWordLength)}
	}
	def frequency(words : RDD[String], textLength : Long) : Array[(String, Double)] = {
		words.map((word : String) => (word, 1))
							.reduceByKey(_ + _)
								.map{item : (String, Int) => (item._1, item._2.toDouble / textLength)}
									.sortByKey()
										.collect
	}

	def consolidate(words : RDD[(String)], dictionary : Array[(String, Double)]) : Array[(String, Double)] = {
		words.map{word : String =>
			var doc = Double.MaxValue
			val words = dictionary.map(_._1)
			val wordIndex = Text.binSearch(words, word)

			for (num <- 1 to word.length - 1){
				val leftWord = word.substring(0, num)
				val rightWord = word.substring(num)

				val leftWordIndex = Text.binSearch(words, leftWord)
				val rightWordIndex = Text.binSearch(words, rightWord)
				if( leftWordIndex >= 0 && rightWordIndex >= 0 ){
					doc = math.min(doc, dictionary(wordIndex)._2  /
						(dictionary(leftWordIndex)._2 * dictionary(rightWordIndex)._2))
				}
			}

			(word, doc)
		}.collect
	}
}
