package lib

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-7.
 */
object DiggerLibrary {
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
			return (-1, 0)
		}

		return (L,  dictionary(L)._2)
	}


	def dateRange(fromdate : String, todate : String): ArrayBuffer[String] ={
		val ret : ArrayBuffer[String] = new ArrayBuffer[String]()
		val sdf : SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

		val startCalendar : Calendar = Calendar.getInstance()
		val endCalendar : Calendar = Calendar.getInstance()
		startCalendar.setTime(sdf.parse(fromdate))
		endCalendar.setTime(sdf.parse(todate))

		while (startCalendar.compareTo(endCalendar) <= 0){
			ret += sdf.format(startCalendar.getTime)
			startCalendar.add(Calendar.DATE, 1)
		}

		return ret
	}
}
