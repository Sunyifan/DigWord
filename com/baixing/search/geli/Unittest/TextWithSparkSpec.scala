package com.baixing.search.geli.Unittest

import com.baixing.search.geli.Util.{Text, Spark}
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec

/**
 * Created by abzyme-baixing on 14-12-23.
 */
class TextWithSparkSpec extends FlatSpec{
	var sc : SparkContext = _
	Spark.silenceSpark()


	it should "success in spark test" in {
		sc = new SparkContext("local", "test")
		try {
			assert(sc.parallelize(1 until 3).collect().sameElements(Array(1, 2)))
		} finally {
			sc.stop()
		}
	}

	val testString1 = "（出售 ）急 急 繁华地段学区房无税置业首选"
	val testString2 = "售价：75万元（9375元/㎡）\\n户型：2室1厅1卫\\n" +
		"楼层：3/6层\\n面积：80.00㎡\\n朝向：南\\n装修：简单装修\\n" +
		"低价出售，高端小区，学区房，满5年无税，置业首选。"

	val preprocessed1 = Array("出售", "急", "急", "繁华地段学区房无税置业首选")
	val preprocessed2 = Array("售价", "75万元", "9375元/㎡", "户型", "2室1厅1卫",
		"楼层", "3/6层", "面积", "80.00㎡", "朝向", "南",
		"装修", "简单装修", "低价出售", "高端小区", "学区房",
		"满5年无税", "置业首选")

	it should "success in preprocessing with spark" in {
		sc = new SparkContext("local", "test")

		try {
			val res = sc.parallelize(Array(testString1, testString2))
				.flatMap{ str => Text.preproccess(str)}
				.collect()
			assert(res.sameElements(preprocessed1 ++ preprocessed2))
		} finally {
			sc.stop()
		}
	}

	val testString3 = "挖蛤蜊"
	it should "success in split word split word with spark" in {
		sc = new SparkContext("local", "test")

		try {
			val res1 = sc.parallelize(Array(testString3)).flatMap{str => Text.splitWord(str, 1)}.collect()
			val res2 = sc.parallelize(Array(testString3)).flatMap{str => Text.splitWord(str, 2)}.collect()
			val res3 = sc.parallelize(Array(testString3)).flatMap{str => Text.splitWord(str, 3)}.collect()
			val res4 = sc.parallelize(Array(testString3)).flatMap{str => Text.splitWord(str, 5)}.collect()

			assert(res1.sameElements(Array("挖", "蛤","蜊")))
			assert(res2.sameElements(Array("挖", "挖蛤", "蛤" , "蛤蜊", "蜊")))
			assert(res3.sameElements(Array("挖", "挖蛤", "挖蛤蜊", "蛤" , "蛤蜊", "蜊")))
			assert(res4.sameElements(Array("挖", "挖蛤", "挖蛤蜊", "蛤" , "蛤蜊", "蜊")))

		} finally {
			sc.stop()
		}
	}
}
