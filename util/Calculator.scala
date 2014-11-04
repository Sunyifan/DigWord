package util

import lib.Searcher
import scala.collection.mutable.Map
/**
 * Created by abzyme-baixing on 14-11-4.
 */
object Calculator {
	//计算凝结度
	def countDoc(word: (String , Double), TextLen: Int ,  dictionary : Array[(String ,Double)]): (String, Double) = {
		var doc = TextLen.toDouble
		val len = dictionary.length
		for (num <- 1 to word._1.length-1){
			val Lword = word._1.substring(0, num)
			val Rword = word._1.substring(num)
			val searchDicLword = Searcher.BinarySearch(Lword, dictionary, 0, len)
			val searchDicRword = Searcher.BinarySearch(Rword, dictionary, 0, len)
			if( searchDicLword._1 != -1 && searchDicRword._1 != -1){
				doc = math.min(doc, word._2.toDouble * TextLen /searchDicLword._2.toDouble/ searchDicRword._2.toDouble)
			}
		}

		(word._1, doc)
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
}
