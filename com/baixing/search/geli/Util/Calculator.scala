package com.baixing.search.geli.Util

import scala.collection.mutable.Map

/**
 * Created by abzyme-baixing on 14-11-4.
 */
object Calculator {
	//计算凝结度
	def countDoc(word: (String , Double),  dictionary : Array[(String ,Double)]): (String, Double) = {
		var doc = Double.MaxValue

		for (num <- 1 to word._1.length - 1){
			val leftWord = word._1.substring(0, num)
			val rightWord = word._1.substring(num)
			val leftWordIndex = Text.binSearch(dictionary.map(_._1), leftWord)
			val rightWordIndex = Text.binSearch(dictionary.map(_._1), rightWord)
			if( leftWordIndex >= 0 && rightWordIndex >= 0){
				doc = math.min(doc, word._2.toDouble  /
					(dictionary(leftWordIndex)._2.toDouble * dictionary(rightWordIndex)._2.toDouble))
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
			freedom -= v.toDouble / wordList.length.toDouble * Math.log(v.toDouble / wordList.length.toDouble)
		}

		freedom
	}
}
