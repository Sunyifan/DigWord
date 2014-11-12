package com.baixing.search.geli.Util

/**
 * Created by abzyme-baixing on 14-11-12.
 */


object Array{
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