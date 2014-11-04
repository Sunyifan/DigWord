package digger


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.util.Sorting

object MyDigger {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("MyDigger")
		val sc = new SparkContext(conf)


		// arg0 输入文件
		// arg1 频率阈值
		// arg2 凝结度阈值
		// arg3 自由熵阈值
		val srcFile = args(0)
		val frequencyThreshold = args(1).toDouble
		val consolidateThreshold = args(2).toDouble
		val freedomThreshold = args(3).toDouble


		// 读取所有文本
		val sourceFile = sc.textFile(srcFile)
		val distLines = sourceFile.flatMap(line => preproccess(line))
		val words = distLines.flatMap(line => splitWord(line, 5))


		// 计算总长度
		val textLength = distLines.map((line : String) => line.length).reduce(_ + _)


		// 计算词频
		val frequencyRDD = words.map((word : String) => (word, 1))
									.reduceByKey(_ + _)
										.map{item : (String, Int) => (item._1, item._2.toDouble / textLength)}
		// 过滤低词频
		val filteredFrequencyRDD = frequencyRDD.filter{item : (String, Double) => item._2 > frequencyThreshold}


		// 计算凝结度前的准备，取过滤后的此表
		val dictionary = filteredFrequencyRDD.collect()
		Sorting.quickSort(dictionary)(Ordering.by[(String, Double), String](_._1))
		val broadforwardDic = sc.broadcast(dictionary)
		filteredFrequencyRDD.persist()
		filteredFrequencyRDD.map(line => countDoc(line, textLength, dictionary))

		// 计算自由熵
		// 不借助词表来计算自由熵
		val wordPrefix = words.filter{word => word.length > 1}
								.map((word : String) => (word.substring(1), word.charAt(0).toString))
									.reduceByKey(_ + "|" + _ )
										.map{case (word : String, prefixList : String) => (word, freedom(prefixList))}

		val wordSuffix = words.filter{word => word.length > 1}
								.map((word : String) => (word.substring(0,word.length - 1), word.charAt(word.length - 1).toString()))
									.reduceByKey(_ + "|" + _ )
										.map{case (word : String, suffixList : String) => (word, freedom(suffixList))}

		val freedomRDD = wordPrefix.cogroup(wordSuffix).map { item =>
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
	}


	//文本中出现明文的\r\n等转义符号
	def dealWithSpecialCase(content: String): ArrayBuffer[String] = {
		val patterns = Array("\\\\r\\\\n", "\\\\n", "\\\\t", "[0-9]{8,}");
		val tmp = ArrayBuffer[String]()
		val ret = ArrayBuffer[String]()
		ret += content
		for (pat <- patterns) {
			tmp.clear()
			for (ele <- ret) {
				val e = ele.trim()
				if (e != "") {
					tmp ++= e.replaceAll(pat, "|").split( """\|""")
				}
			}
			ret.clear()
			ret ++= tmp.clone()
		}
		ret
	}
	//判断符号是否有意义
	def isMeaningful(ch: Char): Boolean = {
		var ret = false
		val meaningfulMarks = Array('*', '-', 'X', '.','\\')
		if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
			ret = true
		ret
	}
	//把句子按标点符号分隔成小短句，同时过滤掉没有意义的符号
	def preproccess(content: String): ArrayBuffer[String] = {
		val ret = ArrayBuffer[String]()
		val puncs = Array(',', '，', '.', '。', '!', '！', '?', '？', ';', '；', ':', '：', '\'', '‘', '’', '\"', '”', '“', '、', '(', '（', ')', '）', '<', '《', '>', '》', '[', '【', '】', ']', '{', '}', ' ', '\t', '\r', '\n') // 标点集合
		var tmp = ""
		var i = 0
		var before = ' '
		for (ch <- content) {
			i += 1
			if (ch == '.' && Character.isDigit(before) && i < (content.length) && Character.isDigit(content.charAt(i))) {
				tmp += ch
			}
			else if (puncs.contains(ch)) {
				if (tmp != "") {
					ret ++= dealWithSpecialCase(tmp)
					tmp = ""
				}
			}
			else {
				if (isMeaningful(ch)) tmp += ch
				if (i == content.length) {
					ret ++= dealWithSpecialCase(tmp)
					tmp = ""
				}
			}
			before = ch
		}
		ret
	}

	def splitWord(v: String, wordLength: Int ):ArrayBuffer[String] = {
		val len = v.length
		//textLength += len
		val greetStrings =  ArrayBuffer[String]()
		for (i <- 0 to len - 1) {
			// 单词起始点位置
			var j: Int = 1 // 新词长度
			while (i + j <= len && j <= wordLength) {
				val tmp: String = v.substring(i, i + j)
				greetStrings += tmp
				j += 1
			}
		}
		greetStrings
	}

	def freedom(wordListInString : String) : Double = {
		val wordList = wordListInString.split("|")
		var count = Map[String, Int]()
		var freedom : Double = 0.0

		for(s <- wordList){
			if (!count.contains(s))
				count += (s -> 0)

			count(s) = count(s) + 1
		}


		for((k, v) <- count){
			freedom = freedom - v.toDouble / wordList.length.toDouble * Math.log(v.toDouble / wordList.length.toDouble)
		}

		return freedom
	}


	//从广播变量中，用二分查找，查找到某个词的位置和频率
	def BinarySearch(word: String , dictionary : Array[(String ,Double)], from: Int = 0, to: Int) : (Int, Double) = {
		var L = from
		var R = to - 1
		var mid : Int = 0
		while (L < R) {
			mid = (L + R) / 2
			if (word > dictionary(mid)._1)
				L = mid + 1
			else
				R = mid
		}
		if (word != dictionary(L)._1){
			println("NOT FIND WORD" + word)
			return (-1, 0)
		}
		return (L,  dictionary(L)._2)
	}
	//计算凝结度
	def countDoc(word: (String , Double), TextLen: Int ,  dictionary : Array[(String ,Double)]): (String , (Double ,Double)) = {
		var tmp = TextLen.toDouble
		val len = dictionary.length
		for (num <- 1 to word._1.length-1){
			val Lword = word._1.substring(0, num)
			val Rword = word._1.substring(num)
			val searchDicLword = BinarySearch(Lword, dictionary, 0, len)
			val searchDicRword = BinarySearch(Rword, dictionary, 0, len)
			if( searchDicLword._1 == -1 || searchDicRword._1 == -1){
				println("Lword: " + Lword)
				println("Rword: " + Rword)
				println("No words found")
			}
			else{
				tmp = math.min(tmp, word._2.toDouble * TextLen /searchDicLword._2.toDouble/ searchDicRword._2.toDouble)
				/*if (tmp > word._2.toDouble * TextLen.toDouble / searchDicLword._2.toDouble / searchDicRword._2.toDouble){
				  realL = Lword
				  realR = Rword
				  realLfreq = searchDicLword._2
				  realRfreq = searchDicRword._2
				  tmp =  word._2 * TextLen * 1.0 / (searchDicLword._2* searchDicRword._2)
				}*/
			}
		}
		if(tmp < 0)
			println(word)
		(word._1 , (word._2, tmp))
	}


	def filterThreshold(item : (String, Double), threshold : Double): Boolean ={
		if(item._2 > threshold)
			true
		else
			false
	}
}