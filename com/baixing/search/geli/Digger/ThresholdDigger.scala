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

	def words(rawText : RDD[String], maxWordLength : Int = 10) : RDD[String] = {
		processedText(rawText).flatMap{line : String => Text.splitWord(line, maxWordLength)}
	}

	def frequency(words : RDD[String], textLength : Long) : RDD[(String, Double)] = {
		words.map((word : String) => (word, 1))
				.reduceByKey(_ + _)
					.map{item : (String, Int) => (item._1, item._2.toDouble / (textLength - item._1.length + 1))}
						.sortByKey()
	}

	def consolidate(words : RDD[String], dictionary : Array[(String, Double)]) : RDD[(String, Double)] = {
		words.filter(_.length > 1)
				.map{word : String =>
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
				}
	}

	def freedom(words : RDD[String]): RDD[(String, Double)] ={
		val leftFreedom = words.filter(_.length > 1)
									.map{word : String => (word.substring(1, word.length), word.charAt(0))}
										.groupByKey
											.map{ item : (String, Iterable[Char]) => (item._1, Text.entrophy(item._2.toArray))}
		val rightFreedom = words.filter(_.length > 1)
									.map{word : String => (word.substring(0, word.length - 1), word.charAt(word.length - 1))}
										.groupByKey
											.map{ item : (String, Iterable[Char]) => (item._1, Text.entrophy(item._2.toArray))}

		leftFreedom.cogroup(rightFreedom)
						.map{
							item : (String, (Iterable[Double], Iterable[Double])) =>
								val arr1 = item._2._1.toArray
								val arr2 = item._2._2.toArray
								if (arr1.length > 0 && arr2.length > 0)
									(item._1, Math.min(arr1(0), arr2(0)))
								else if(arr1.length > 0)
									(item._1, arr1(0))
								else
									(item._1, arr2(0))

						}
	}

	def dig(rawText : RDD[String], freqThres : Double = 10e-7,
		                                consolThres : Double = 10,
		                                    freeThres : Double = 0.5): RDD[(String, (Double, Double, Double))] ={
		val len = textLength(rawText)
		val word = words(rawText)

		val freq = frequency(word, len).filter(_._2 > freqThres)
		val consol = consolidate(freq.keys, freq.collect).filter(_._2 > consolThres)
		val free = freedom(word).filter(_._2 > freeThres)

		freq.join(consol)
				.join(free)
					.map{
						item : (String, ((Double, Double), Double)) =>
							(item._1, (item._2._1._1, item._2._1._2, item._2._2))
					}.sortByKey()
	}

	def threshold(arr : Array[Double], percentage : Double) : Double = {
		arr.sortWith(_ > _)((arr.length * percentage).toInt)
	}
}
