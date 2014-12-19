package com.baixing.search.geli.Digger

import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import com.baixing.search.geli.Util.Text

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object ThresholdDigger {
	def textLength(text : RDD[String]) : Long = text.map(_.length).reduce(_ + _)

	def processedText(text : RDD[String]) : RDD[String] = text.flatMap{item : String => Text.preproccess(item)}

	def words(processedText : RDD[String], maxWordLength : Int = 10) : RDD[String] = {
		processedText.flatMap{line : String => Text.splitWord(line, maxWordLength)}
	}

	def frequency(words : RDD[String], textLength : Long) : RDD[(String, Double)] = {
		words.map((word : String) => (word, 1))
				.reduceByKey(_ + _)
					.map{item : (String, Int) => (item._1, item._2.toDouble / (textLength - item._1.length + 1))}
	}

	def newConsolidate(words : RDD[String], frequency : RDD[(String, Double)]): RDD[(String, Double)] = {
		val temp = words.filter(_.length > 1).flatMap {
			word =>
				val ret = new ArrayBuffer[(String, String)]

				for (num <- 1 to word.length - 1) {
					val leftWord = word.substring(0, num)
					val rightWord = word.substring(num)

					ret += ((leftWord, word))
					ret += ((rightWord, word))

				}

				ret += ((word, word))
				ret
		}

		temp.groupByKey().join(frequency).flatMap{
			item : (String, (Iterable[String], Double)) =>
				val ret = new ArrayBuffer[(String, (String, Double))]

				for (s <- item._2._1.toArray){
					ret += ((s, (item._1, item._2._2)))
				}

				ret
		}.groupByKey().map{
			item : (String, Iterable[(String, Double)]) =>
				val word = item._1
				val freq = item._2.toMap

				var consolidate = Double.MaxValue

				for (num <- 1 to word.length - 1){
					val leftWord = word.substring(0, num)
					val rightWord = word.substring(num)

					val leftWordIndex = freq(leftWord)
					val rightWordIndex = freq(rightWord)
					if( leftWordIndex >= 0 && rightWordIndex >= 0 ){
						consolidate = math.min(consolidate, freq(word)  /
							(freq(leftWord) * freq(rightWord)))
					}
				}

				(item._1, consolidate)
		}
	}

	def consolidate(words : RDD[String], frequency : RDD[(String, Double)]) : RDD[(String, Double)] = {
		val dictionary = frequency.collect().toMap
		val broadcastedDict = Env.sparkContext().broadcast(dictionary)


		frequency.filter(_._1.length > 1).map{
			item : (String, Double)=>
			val word = item._1
			var consolidate = Double.MaxValue

			for (num <- 1 to word.length - 1) {
				val leftWord = word.substring(0, num)
				val rightWord = word.substring(num)

				val leftWordIndex = broadcastedDict.value(leftWord)
				val rightWordIndex = broadcastedDict.value(rightWord)
				if( broadcastedDict.value.contains(leftWord) && broadcastedDict.value.contains(rightWord) ){
					consolidate = math.min(consolidate, broadcastedDict.value(leftWord)  /
						(broadcastedDict.value(rightWord) * broadcastedDict.value(rightWord)))
				}
			}

			(word, consolidate)
		}
	}

	def freedom(words : RDD[String], freeThres : Double = 0.8): RDD[(String, Double)] ={
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

						}.filter(_._1.length > 1)
	}

	def dig(rawText : RDD[String], freqThres : Double = 10e-7,
	                                    consolThres : Double = 100,
		                                    freeThres : Double = 0.8): RDD[(String, (Double, Double, Double))] ={
		val len = textLength(rawText)
		val text = processedText(rawText)
		val word = words(text)

		val freq = frequency(word, len).filter(_._2 > freqThres)
		val consol = consolidate(word, freq).filter(_._2 > consolThres)
		val free = freedom(word).filter(_._2 > freeThres)

		freq.join(consol).join(free).map{
			item : (String, ((Double, Double), Double))
				=> (item._1, (item._2._1._1, item._2._1._2, item._2._2))
		}.sortByKey().filter{item => item._1.length > 1 && item._1.length < 5}
	}
}
