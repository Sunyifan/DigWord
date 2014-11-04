package util

import lib.TextTransformer

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-4.
 */
object TextProcessor {
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
					ret ++= TextTransformer.dealWithSpecialCase(tmp)
					tmp = ""
				}
			}
			else {
				if (TextTransformer.isMeaningful(ch)) tmp += ch
				if (i == content.length) {
					ret ++= TextTransformer.dealWithSpecialCase(tmp)
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

}
