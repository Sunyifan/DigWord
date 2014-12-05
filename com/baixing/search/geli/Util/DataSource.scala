package com.baixing.search.geli.Util

import com.baixing.search.geli.Environment.Env
import org.apache.spark.sql.SchemaRDD

/**
 * Created by abzyme-baixing on 14-12-5.
 */


// 从hive中取原始数据，只能被数据预处理层以外的代码看见
object DataSource {
	private val category = Env.getProperty("category")
	private val areaid = Env.getProperty("area_id")
	private val fromdate = Env.getProperty("fromdate")
	private val todate = Env.getProperty("todate")

	private def execute(sql : String) : SchemaRDD = {
		Env.hiveContext().sql(sql)
	}

	private def AdQuery () : String = {
		"\nSELECT\n" +
			"    ad_id,\n " +
			"    title,\n" +
			"    content,\n"  +
			"    tags\n" +
			"FROM\n" +
			"    logs.ad_content\n" +
			"WHERE\n" +
			"    category = '" + category + "'\n" +
			"    and area_id = '" + areaid + "'\n" +
			"    and dt between " + fromdate + " and " + todate
	}

	private def UserActionQuery() : String = {
		"\nSELECT\n" +
			"   visitor_id,\n" +
			"   visitor\n" +
			"   referer,\n" +
			"   landing\n" +
			"FROM\n" +
			"   base.user_actions\n" +
			"WHERE\n" +
			"   and landing['category_name_en'] = '" +  category+ "'\n" +
			"   and landing['city_id'] = '" + areaid + "'" +
			"   dt between '" + fromdate + "' and '" + todate + "'\n" +
			"   and platform in ('wap','web')"
	}
}
