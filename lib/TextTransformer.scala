package lib

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-4.
 */
object TextTransformer {
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
}
