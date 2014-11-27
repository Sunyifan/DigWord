package com.baixing.search.geli.Util

import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD

/**
 * Created by abzyme-baixing on 14-11-27.
 */
object Data {
	// ad interface
	def adContentWithId() : RDD[(String, String, String)] ={
		RawAd().map{ row => (row(0).toString, row(1).toString + " " + row(2).toString, row(3).toString)}
	}

	def adContent() : RDD[String] = {
		RawAd().map{ row => (row(1).toString + " " + row(2).toString)}
	}

	private def RawAd () : SchemaRDD = {
		Env.hiveContext().sql(AdQuery())
	}

	private def AdQuery () : String = {
		val category = Env.getProperty("category")
		val areaid = Env.getProperty("area_id")
		val fromdate = Env.getProperty("fromdate")
		val todate = Env.getProperty("todate")

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


	// user action interface
	def adVisitor(): RDD[(String, String)] ={
		RawUserAction().filter{
			row => row(2).toString != "0"
		}.map{
			row => (row(2).toString, row(0).toString)
		}
	}

	def query(): RDD[String] = {
		RawUserAction().filter{
			row => row(1) != null && row(1).toString.indexOf("query=") >= 0
		}.map{
			row =>
				row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6)
		}.filter(_.length > 0)
	}

	def UserActionBeforeVad(): RDD[(String, String, String)] ={
		RawUserAction().filter{ row => row(2).toString != "0" && row(1).toString.indexOf("query=") >= 0}
			.map{ row =>
			val visitor_id = row(0).toString
			val query = row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6)
			val ad_id = row(2).toString
			(visitor_id, query, ad_id)}.filter(_._2.length != 0)
	}

	def RawUserAction() : SchemaRDD = {
		Env.hiveContext().sql(UserActionQuery())
	}

	def UserActionQuery() : String = {
		val category = Env.getProperty("category")
		val areaid = Env.getProperty("area_id")
		val fromdate = Env.getProperty("fromdate")
		val todate = Env.getProperty("todate")


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


	// temp interface for geli
	private val root = "/user/sunyifan/geli/"
	def geli() : RDD[String] = {
		val geliPath = root + Env.getProperty("category") + "/" + Env.getProperty("fromdate") + "-" +
													Env.getProperty("todate") + "-" + Env.getProperty("area_id")
		Env.sparkContext().textFile(geliPath)
	}
}
