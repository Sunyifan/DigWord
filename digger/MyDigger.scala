package digger


import lib.Searcher
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import util.{Calculator, TextProcessor}
import scala.util.Sorting

object MyDigger {
	// args:
	// 0. 任务名
	// 1. 输入路径
	// 2. 频率阈值
	// 3. 凝结度阈值
	// 4. 自由熵阈值
	// 5. 词长上界
	def main(args : Array[String]): Unit ={
		val conf = new SparkConf().setAppName(args(0))
		val sc = new SparkContext(conf)

		val numPartitions = args(1).toInt
		val inputPath = args(2)
		val frequencyThreshold = args(3).toDouble
		val consolidateThreshold = args(4).toDouble
		val freedomThreshold = args(5).toDouble
		val maxWordLength = args(6).toInt
		val outputPath = args(7)


		// 初始化文件
		val distFile = sc.textFile(inputPath, numPartitions)

		// 预处理文本: 1. 去除特殊的转义符号 2. 把全文切分成短句 3. 计算总文本长度
		val distLines = distFile.flatMap(line => TextProcessor.preproccess(line))
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


		// part-2:  计算凝结度并过滤
		// 准备计算凝结度, 1. 生成词典并排序 2. 广播词典 3. 计算凝结度 4. 过滤
		val consolidateRDD = filteredFrequencyRDD.map(line => Calculator.countDoc(line, textLength, dictionary))
		val filteredConsolidateRDD = consolidateRDD.filter{item : (String, Double) => item._2 > consolidateThreshold}


		// part-3:  不借助词表来计算自由熵
		// 生成此前后缀
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
												.map {item : (String, Double) => (item._1.substring(0, item._1.length - 2), item._1.charAt(0).toString)}
													.reduceByKey(_ + "|" + _)
														.map{case (word : String, suffixList : String) => (word, Calculator.freedom(suffixList))}

		// 计算自由熵
		val freedomRDD = leftFreedomRDD.cogroup(rightFreedomRDD).map { item =>
			val left = item._2._1.toArray;
			val right = item._2._2.toArray;


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

		filteredWords.saveAsTextFile(outputPath)
	}
}