package com.baixing.search.geli.Util

import scala.collection.mutable.ArrayBuffer

object Text {
	private val stopString = Array("\\", "\\r", "\\n")
	private val reservedString = Array("㎡")

	private def isValidChar(c : Character): Boolean ={
		Character.isAlphabetic(c.toInt) || Character.isDigit(c) || reservedString.contains(c.toString)
	}

	private def removeStopString(str : String) : Array[String] = {
		var ret = str
		for (s <- stopString){
			ret = ret.replace(s, " ")
		}

		ret.split(" ").filter(_.length != 0)
	}

	def preproccess(content: String): ArrayBuffer[String] = {
		val ret = new ArrayBuffer[String]
		var start, end = 0

		while(start != content.length){
			end = start
			val buf = new StringBuilder

			while( end != content.length && isValidChar(content(end))){
				buf.append(content(end))
				end += 1
			}

			if (buf.length != 0)
				ret ++= removeStopString(buf.toString.toLowerCase)

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
				var tmp: String = v.substring(i, i + j)
				var flag =true
				var ti=i
				var tj=j
				while(tmp(0).isLetterOrDigit && flag) {
					if (ti>0 && v(ti).isLetterOrDigit)
						tmp=v(ti)+tmp
					else
						flag=false
				}
				flag = true
				while(tmp(tmp.length-1).isLetterOrDigit && flag) {
					if (tj<v.length && v(tj).isLetterOrDigit)
						tmp+=v(tj)
					else
						flag=false
				}
				greetStrings += tmp
				j += 1
			}
		}
		greetStrings
	}
}
