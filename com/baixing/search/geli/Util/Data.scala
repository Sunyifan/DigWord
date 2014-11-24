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
	def adInput(conf : Configuration, env : Env) : RDD[String] = {
		RawAd(conf, env).map{ row => row(1).toString + " " + row(2).toString}
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
	def UserActionInputRDD(conf : Configuration, env : Env) : RDD[(String, String)] = {
		UserAction(conf, env).map{ row =>
			val visitor_id = row(0).toString
			val query = row(1).toString.split("\\,").filter(item => item.startsWith("query"))(0).substring(6)
			val ad_id = row(2).toString

			(visitor_id + "|" + ad_id, query)
		}.filter(item => item._2.length != 0)
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
		"    referer['query'],\n" +
		"    landing['ad_id']\n" +
		"FROM\n" +
		"    base.user_actions\n" +
		"WHERE\n" +
		"    dt between " + fromdate + " and " + todate + "\n" +
		"    and referer['url'] not like '%select%'\n" +
		"    and referer['query'] like '%query=%'\n" +
		"    and landing['ad_id'] <> 0\n" +
		"    and landing['city_id'] = '" + areaid + "'\n" +
		"    and landing['category_name_en'] = '" + category + "'\n" +
		"    and landing['url_type'] = 4\n" +
		"    and (platform = 'wap' or platform = 'web')\n"
	}



	// chuanzhu UV

	def ad2VisitorInputRDD(conf : Configuration, env : Env): RDD[(String, String)] ={
		ad2Visitor(conf, env).map{ row => (row(1).toString, row(0).toString)}
	}

	def ad2Visitor(conf : Configuration, env : Env): SchemaRDD ={
		env.hiveContext().hql(ad2VistorQuery(conf))
	}

	def ad2VistorQuery(conf : Configuration) : String = {
		val category = conf.category
		val areaid = conf.areaId
		val fromdate = conf.fromdate
		val todate = conf.todate


		"\nSELECT\n" +
			"    visitor_id,\n" +
			"    landing['ad_id']\n" +
			"FROM\n" +
			"    base.user_actions\n" +
			"WHERE\n" +
			"    dt between " + fromdate + " and " + todate + "\n" +
			"    and referer['url'] not like '%select%'\n" +
			"    and landing['ad_id'] <> 0\n" +
			"    and landing['city_id'] = '" + areaid + "'\n" +
			"    and landing['category_name_en'] = '" + category + "'\n" +
			"    and landing['url_type'] = 4\n" +
			"    and (platform = 'wap' or platform = 'web')\n"
	}



	// get adtag
	def adTagInputRDD(conf : Configuration, env : Env) : RDD[(String, String)] = {
		adTag(conf, env).map{
			row =>
				(row(0).toString, row(1).toString)
		}
	}

	def adTag(conf : Configuration, env : Env): SchemaRDD = {
		env.hiveContext().hql(adTagQuery())
	}

	def adTagQuery(): String ={
		"\nSELECT\n" +
		"    *\n" +
		"FROM\n" +
		"   logs.ad_tag"
	}



	// get all tag
	def allTag(conf : Configuration, env : Env) : Array[String] = {
		(tag(conf, env) ++ tagAlias(conf, env)).sortWith(_ < _)
	}

	def tag(conf : Configuration, env : Env) : Array[String] = {
		env.hiveContext().hql(tagQuery()).map{
			row => row(0).toString
		}.collect.distinct.sortWith(_ < _)
	}


	def tagAlias(conf : Configuration, env : Env) : Array[String] = {
		env.hiveContext().hql(tagAliasQuery()).map{
			row => row(0).toString
		}.collect.distinct.sortWith(_ < _)
	}

	def tagQuery() : String = {
		"\nSELECT\n" +
		"    name\n" +
		"  FROM\n" +
		"    logs.tagn"
	}

	def tagAliasQuery() : String = {
		"\nSELECT\n" +
			"    codeName\n" +
			"  FROM\n" +
			"    logs.tag_alias"

	}
}
