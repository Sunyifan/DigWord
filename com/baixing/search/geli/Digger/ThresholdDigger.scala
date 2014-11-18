package com.baixing.search.geli.Digger

import lib.Searcher
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import util.{Calculator, TextProcessor}

import scala.util.Sorting

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object ThresholdDigger {
	def dig(inputRDD : RDD[(String, String)], frequencyThreshold : Double = 0.0001, consolidateThreshold : Double = 100,
	        freedomThreshold : Double = 0.9, maxWordLength : Int = 10): Array[String] = {
		// 预处理文本: 1. 去除特殊的转义符号 2. 把全文切分成短句 3. 计算总文本长度
		val distLines = inputRDD.flatMap{item : (String, String) => TextProcessor.preproccess(item._2)}
		val textLength = distLines.map(line => line.length).reduce(_ + _)

		// 生成词表
		val distWords = distLines.flatMap(line => TextProcessor.splitWord(line, maxWordLength))

		// part-1:  计算词频并过滤
		val frequencyRDD = distWords.map((word : String) => (word, 1))
			.reduceByKey(_ + _)
			.map{item : (String, Int) => (item._1, item._2.toDouble / textLength)}
		val filteredFrequencyRDD = frequencyRDD.filter{item : (String, Double) => item._2 > frequencyThreshold}
		filteredFrequencyRDD.persist()


		val dictionary = filteredFrequencyRDD.collect()

		Sorting.quickSort(dictionary)(Ordering.by[(String, Double), String](_._1))

		val extendedRDD = distLines.filter{line : String =>
			Searcher.BinarySearch(line, dictionary, 0, dictionary.length)._1 >= 0
		}.map{line : String => (line, Searcher.BinarySearch(line, dictionary, 0, dictionary.length)._2)}

		// part-2:  计算凝结度并过滤
		// 准备计算凝结度, 1. 生成词典并排序 2. 广播词典 3. 计算凝结度 4. 过滤
		val consolidateRDD = filteredFrequencyRDD.map(line => Calculator.countDoc(line, textLength, dictionary))
		val extendedConsolidateRDD = extendedRDD.map(line => Calculator.countDoc(line, textLength, dictionary))

		val filteredConsolidateRDD = consolidateRDD.filter{item : (String, Double) => item._2 > consolidateThreshold}
		val filteredExtendedConsolidateRDD = extendedConsolidateRDD.filter{item : (String, Double) => item._2 > consolidateThreshold}

		// part-3:  过滤得到前后缀，计算自由熵
		val leftFreedomRDD = frequencyRDD.filter{
			item : (String, Double) =>
				item._1.length > 1 &&
					Searcher.BinarySearch(item._1.substring(1), dictionary, 0, dictionary.length)._1 >=0
		}
			.map {item : (String, Double) => (item._1.substring(1), item._1.charAt(0).toString)}
			.reduceByKey(_ + "|" + _)
			.map{case (word : String, prefixList : String) => (word, Calculator.freedom(prefixList))}


		val rightFreedomRDD = frequencyRDD.filter{
			item : (String, Double) =>
				item._1.length > 1 &&
					Searcher.BinarySearch(item._1.substring(0, item._1.length - 2), dictionary, 0, dictionary.length)._1 >=0
		}
			.map {item : (String, Double) => (item._1.substring(0, item._1.length - 1), item._1.charAt(0).toString)}
			.reduceByKey(_ + "|" + _)
			.map{case (word : String, suffixList : String) => (word, Calculator.freedom(suffixList))}

		// 计算自由熵
		val freedomRDD = leftFreedomRDD.cogroup(rightFreedomRDD).map { item =>
			val left = item._2._1.toArray
			val right = item._2._2.toArray


			if (left.length > 0 && right.length > 0)
				(item._1, Math.min(left(0), right(0)))
			else if(left.length > 0)
				(item._1, left(0))
			else
				(item._1, right(0))
		}

		val filteredFreedomRDD = freedomRDD.filter{ item : (String, Double) => item._2 > freedomThreshold}

		// 计算过滤后存在的词
		val filteredWords = filteredFrequencyRDD.keys
			.intersection(filteredConsolidateRDD.keys)
			.intersection(filteredFreedomRDD.keys)
			.filter(word => word.length > 1)
			.distinct

		val extendedWords = extendedRDD.keys
			.intersection(filteredExtendedConsolidateRDD.keys)
			.filter(word => word.length > 1)
			.distinct

		filteredWords.union(extendedWords).collect
	}

}
