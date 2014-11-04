package lib

/**
 * Created by abzyme-baixing on 14-11-4.
 */
object Searcher {
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

}
