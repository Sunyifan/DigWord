package com.baixing.search.geli.Util

import com.baixing.search.geli.Configuration.Configuration
import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD


/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Data {
	// get ad
	def AdInput(conf : Configuration, env : Env) : RDD[(String,String)] = {
		RawAd(conf, env).map{ row => (row(0).toString, row(1).toString + " " + row(2).toString)}
	}

	private def RawAd (conf : Configuration, env : Env) : SchemaRDD = {
		env.hiveContext().hql(AdQuery(conf))
	}

	private def AdQuery (conf : Configuration) : String = {
		val category = conf.category
		val areaid = conf.areaId
		val fromdate = conf.fromdate
		val todate = conf.todate

		"\nSELECT\n" +
		"    ad_id,\n " +
		"    title,\n" +
		"    content\n"  +
		"FROM\n" +
		"    shots.ad_content\n" +
		"WHERE\n" +
		"    category = '" + category + "'\n" +
		"    and area_id = '" + areaid + "'\n" +
		"    and dt between " + fromdate + " and " + todate
	}


	// get user action data
	def UserActionInputRDD(conf : Configuration, env : Env) : RDD[(String, (String, String))] = {
		UserAction(conf, env).map{ row =>
			val visitor_id = row(0).toString
			val query = row(1).toString.split("\\,").filter(item => item.startsWith("query"))(0).substring(6)
			val ad_id = row(2).toString

			(visitor_id, (query, ad_id))
		}.filter(item => item._2._1.length != 0)
	}

	def UserAction(conf : Configuration, env : Env) : SchemaRDD = {
		env.hiveContext().hql(UserActionQuery(conf))
	}

	def UserActionQuery(conf : Configuration) : String = {
		val category = conf.category
		val areaid = conf.areaId
		val fromdate = conf.fromdate
		val todate = conf.todate


		"\nSELECT\n" +
		"    visitor_id,\n" +
		"    referer['query'] as query,\n" +
		"    landing['ad_id'] as ad_id\n" +
		"FROM\n" +
		"    base.user_actions\n" +
		"WHERE\n" +
		"    dt between " + fromdate + " and " + todate + "\n" +
		"    and referer['query'] like '%query%'\n" +
		"    and referer['query'] is not null\n" +
		"    and referer['query'] <> ''\n" +
		"    and referer['url'] not like '%select%'\n" +
		"    and referer['city_id'] = '" + areaid + "'\n" +
		"    and landing['city_id'] = '" + areaid + "'\n" +
		"    and landing['url_type'] = 4\n" +
		"    and (platform = 'wap' or platform = 'web')\n"
	}


	// get chuanzhu word
	def ChuanzhuWord(filename : String, env : Env): Array[String] ={
		env.sparkContext().textFile(filename).collect
	}
}
