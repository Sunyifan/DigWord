package com.baixing.search.geli.Util

import scala.util.control.Breaks

/**
 * Created by abzyme-baixing on 14-12-18.
 */
object Rule {
	def isPearl(word : String, pearlTags : Array[String]): Boolean ={
		pearlTags.contains(word)
	}

	def containPearl(word : String, pearlTags : Array[String]) : Boolean ={
		val loop = new Breaks
		var ret = false

		loop.breakable{
			for (tag <- pearlTags){
				if (word.indexOf(tag) >= 0){
					ret = true
					loop.break()
				}
			}
		}

		ret
	}

	def containChinese(word : String): Boolean ={
		val loop = new Breaks
		var ret = false

		loop.breakable{
			for (ch <- word){
				if (!ch.isDigit && !ch.isLower){
					ret = true
					loop.break()
				}
			}
		}

		ret
	}



	def aboveFreqThres(word : String, freq : Double): Boolean ={
		if (word.length == 2)
			freq > 4e-6
		else if(word.length == 3)
			freq > 3e-6
		else if(word.length == 4 || word.length == 5)
			freq > 2e-6
		else
			false
	}

	def aboveConsolThres(word : String, consol : Double) : Boolean ={
		if (word.length == 2)
			consol > 16
		else if(word.length == 3)
			consol > 13
		else if(word.length == 4)
			consol > 31
		else if(word.length == 5)
			consol > 64
		else
			false
	}

	def aboveFreeThres(word : String, free : Double) : Boolean ={
		if (word.length == 2)
			free > 1.1
		else if(word.length == 3)
			free > 0.9
		else if(word.length == 4)
			free > 0.9
		else if(word.length == 5)
			free > 0.8
		else
			false
	}
}
