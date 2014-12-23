package com.baixing.search.geli.Unittest

import com.baixing.search.geli.Util.SparkUtil
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec

/**
 * Created by abzyme-baixing on 14-12-23.
 */
class TextWithSparkSpec extends FlatSpec{
	var sc : SparkContext = _
	SparkUtil.silenceSpark()


	it should "success in spark test" in {
		sc = new SparkContext("local", "test")
		try {
			assert(sc.parallelize(1 until 3).collect().sameElements(Array(1, 2)))
		} finally {
			sc.stop()
		}
	}


}
