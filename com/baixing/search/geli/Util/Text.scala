package com.baixing.search.geli.Util

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Text {
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

	def isMeaningful(ch: Char): Boolean = {
		var ret = false
		val meaningfulMarks = Array('*', '-', 'X', '.','\\')
		if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
			ret = true
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
}
