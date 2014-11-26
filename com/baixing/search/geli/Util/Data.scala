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
	def adContentWithId(conf : Configuration, env : Env) : RDD[(String, String, String)] ={
		RawAd(conf, env).map{ row => (row(0).toString, row(1).toString + " " + row(2).toString, row(3).toString)}
	}

	def adContent(conf : Configuration, env : Env) : RDD[String] = {
		RawAd(conf, env).map{ row => row(1).toString + " " + row(2).toString}
	}

	def adTag(conf : Configuration, env : Env): RDD[(String, String)] = {
		RawAd(conf, env).map{ row => (row(0).toString, row(3).toString)}
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
		"    content,\n"  +
		"    tags\n" +
		"FROM\n" +
		"    logs.ad_content\n" +
		"WHERE\n" +
		"    category = '" + category + "'\n" +
		"    and area_id = '" + areaid + "'\n" +
		"    and dt between " + fromdate + " and " + todate + "\n" +
		"    and tags is not null"
	}


	// get user action data
	def adVisitor(conf : Configuration, env : Env): RDD[(String, String)] ={
		RawUserAction(conf, env).filter{
			row => row(2).toString != "0"
		}.map{
			row => (row(2).toString, row(0).toString)
		}
	}

	def query(conf : Configuration, env : Env): RDD[String] = {
		RawUserAction(conf, env).filter{
			row => row(1) != null && row(1).toString.indexOf("query=") >= 0
		}.map{
			row =>
				row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6)
		}.filter(_.length > 0)
	}

	def RawUserAction(conf : Configuration, env : Env) : SchemaRDD = {
		env.hiveContext().hql(UserActionQuery(conf))
	}

	def UserActionQuery(conf : Configuration) : String = {
		val category = conf.category
		val areaid = conf.areaId
		val fromdate = conf.fromdate
		val todate = conf.todate


		"\nSELECT\n" +
		"    visitor_id,\n" +
		"    referer['query'],\n" +
		"    landing['ad_id']\n" +
		"FROM\n" +
		"    base.user_actions\n" +
		"WHERE\n" +
		"    dt between " + fromdate + " and " + todate + "\n" +
		"    and referer['url'] not like '%select%'\n" +
		"    and landing['city_id'] = '" + areaid + "'\n" +
		"    and landing['category_name_en'] = '" + category + "'\n" +
		"    and landing['url_type'] = 4\n" +
		"    and (platform = 'wap' or platform = 'web')\n"
	}

	// get geli
	private val root = "/user/sunyifan/geli/"
	def geli(conf : Configuration, env : Env): RDD[String] ={
		val geliPath = root + conf.category() + "/" + conf.fromdate + "-" + conf.todate + "-" + conf.areaId
		env.sparkContext().textFile(geliPath)
	}
}
