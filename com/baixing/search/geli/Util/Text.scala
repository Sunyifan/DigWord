package com.baixing.search.geli.Util

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Text {
	private val reserveChar = Array('*', '-', 'X', '.','\\', '/')
	private val stopString = Array("\\r", "\\n")

	private def isValidChar(c : Character): Boolean ={
		Character.isAlphabetic(c.toInt) || Character.isDigit(c) || reserveChar.contains(c)
	}

	private def removeStopString(str : String) : Array[String] = {
		var ret = str
		for (s <- stopString){
			ret = ret.replace(s, " ")
		}

		ret.split(" ").filter(_.length != 0)
	}

	def preproccess(content: String): ArrayBuffer[String] = {
		val ret = ArrayBuffer[String]()
		var start, end = 0

		while(start != content.length){
			end = start
			val buf = new StringBuilder

			while( end != content.length && isValidChar(content(end))){
				buf.append(content(end))
				end += 1
			}

			if (buf.length != 0)
				ret ++= removeStopString(buf.toString)

			if (start == end)
				end += 1

			start = end
		}

		ret
	}

	def splitWord(v: String, wordLength: Int ):ArrayBuffer[String] = {
		val len = v.length

		val greetStrings =  ArrayBuffer[String]()
		for (i <- 0 to len - 1) {
			var j: Int = 1
			while (i + j <= len && j <= wordLength) {
				val tmp: String = v.substring(i, i + j)
				greetStrings += tmp
				j += 1
			}
		}
		greetStrings
	}

	def find(arr : Array[String], item : String): Int ={
		java.util.Arrays.binarySearch(arr.asInstanceOf[Array[AnyRef]], item)
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
