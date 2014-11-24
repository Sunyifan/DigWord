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
					.map{item : (String, Int) => (item._1, item._2.toDouble / (textLength - item._1.length + 1))}
						.sortByKey()
							.collect
	}

	def consolidate(words : RDD[(String)], dictionary : Array[(String, Double)]) : Array[(String, Double)] = {
		words.map{word : String =>
			var consolidate = Double.MaxValue
			val words = dictionary.map(_._1)
			val wordIndex = Text.find(words, word)

			for (num <- 1 to word.length - 1){
				val leftWord = word.substring(0, num)
				val rightWord = word.substring(num)

				val leftWordIndex = Text.find(words, leftWord)
				val rightWordIndex = Text.find(words, rightWord)
				if( leftWordIndex >= 0 && rightWordIndex >= 0 ){
					consolidate = math.min(consolidate, dictionary(wordIndex)._2  /
						(dictionary(leftWordIndex)._2 * dictionary(rightWordIndex)._2))
				}
			}

			(word, consolidate)
		}.collect
	}

	def freedom(words : RDD[String]): Unit ={
		val leftFreedom = words.filter(_.length > 1)
									.map{word : String => (word.substring(1, word.length), word.charAt(0))}
										.groupByKey
											.map{ item : (String, Iterable[Char]) => (item._1, entrophy(item._2.toArray))}
		val rightFreedom = words.filter(_.length > 1)
									.map{word : String => (word.substring(0, word.length - 1), word.charAt(word.length - 1))}
										.groupByKey
											.map{ item : (String, Iterable[Char]) => (item._1, entrophy(item._2.toArray))}

		leftFreedom.cogroup(rightFreedom)
						.map{
							item : (String, (Iterable[Double], Iterable[Double])) =>
								val arr1 = item._2._1.toArray
								val arr2 = item._2._2.toArray
								if (arr1.length >0 && arr2.length > 0)
									(item._1, Math.min(arr1(0), arr2(0)))
								else if(arr1.length > 0)
									(item._1, arr1(0))
								else
									(item._1, arr2(0))

						}
	}

	def entrophy(charArray : Array[Char]): Double ={
		val len = charArray.length.toDouble
		var charCnt = Map[Char, Int]()
		var ret : Double = 0.0

		for (c <- charArray){
			if (!charCnt.contains(c))
				charCnt += (c -> 0)

			charCnt.updated(c, charCnt(c) + 1)
		}

		for((k, v) <- charCnt){
			ret = ret - v.toDouble / len * Math.log(v.toDouble / len)
		}

		ret
	}
}
