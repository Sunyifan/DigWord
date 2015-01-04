package com.baixing.search.geli.Unittest

import org.scalatest.FlatSpec
import com.baixing.search.geli.Util.Text

/**
 * Created by abzyme-baixing on 14-11-24.
 */
class TextSpec extends FlatSpec{
	val testString1 = "（出售 ）急 急 繁华地段学区房无税置业首选"
	val testString2 = "售价：75万元（9375元/㎡）\\n户型：2室1厅1卫\\n" +
		"楼层：3/6层\\n面积：80.00㎡\\n朝向：南\\n装修：简单装修\\n" +
		"低价出售，高端小区，学区房，满5年无税，置业首选。"

	val preprocessed1 = Array("出售", "急", "急", "繁华地段学区房无税置业首选")
	val preprocessed2 = Array("售价", "75万元", "9375元/㎡", "户型", "2室1厅1卫",
		"楼层", "3/6层", "面积", "80.00㎡", "朝向", "南",
		"装修", "简单装修", "低价出售", "高端小区", "学区房",
		"满5年无税", "置业首选")

	it should "success in preprocessing" in {
		assert(Text.preproccess(testString1).sameElements(preprocessed1))
		assert(Text.preproccess(testString2).sameElements(preprocessed2))
	}

	val testString3 = "挖蛤蜊"
	it should "success in split word" in {
		assert(Text.splitWord(testString3, 2).sameElements(Array("挖", "挖蛤", "蛤" , "蛤蜊", "蜊")))
		assert(Text.splitWord(testString3, 5).sameElements(Array("挖", "挖蛤", "挖蛤蜊", "蛤" , "蛤蜊", "蜊")))
	}
}
