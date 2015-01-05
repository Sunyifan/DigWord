package com.baixing.search.geli.Util

import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.SparkContext._

/**
 * Created by abzyme-baixing on 14-11-27.
 */
object Data {
	// ad interface
	def adTag() : RDD[(String, String)] = {
		RawAd().filter{row => row(3) != null && !row(3).toString.isEmpty}.map{ row => (row(0).toString, row(3).toString)}
	}

	def adContentWithIdAndTag() : RDD[(String, String, String)] ={
		RawAd().filter{row => row(3) != null && !row(3).toString.isEmpty}.map{ row => (row(0).toString, row(1).toString + " " + row(2).toString, row(3).toString)}
	}

	def adContent() : RDD[String] = {
		RawAd().map{ row => (row(1).toString + " " + row(2).toString)}
	}

	def adContentWithId() : RDD[(String, String)] = {
		RawAd().map{ row => (row(0).toString, row(1).toString + " " + row(2).toString)}
	}

	def RawAd () : SchemaRDD = {
		Env.hiveContext().sql(AdQuery()).persist()
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
			"    and dt between " + fromdate + " and " + todate + "\n"
	}



	// user action interface
	def adVisitor(): RDD[(String, String)] ={
		RawUserAction().filter{
			row => row(2).toString != "0"
		}.map{
			row => (row(2).toString, row(0).toString)
		}
	}

	def UserActionWithQuery() : RDD[(String, String)] = {
		RawUserAction().filter{
			row => row(1) != null && row(1).toString.indexOf("query=") >= 0 &&
				row(1).toString.split(",").filter(_.startsWith("query")).length > 0
		}.map{
			row =>
				(row(0).toString, row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6))
		}.filter(_._2.length > 0)

	}

	def UserActionQueryOnly(): RDD[String] = {
		RawUserAction().filter{
			row => row(1) != null && row(1).toString.indexOf("query=") >= 0 &&
				row(1).toString.split(",").filter(_.startsWith("query")).length > 0
		}.map{
			row =>
				row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6)
		}.filter(_.length > 0)
	}

	def UserActionWithAd() : RDD[(String, String)] = {
		RawUserAction().filter{row => row(2).toString != 0}
							.map{ row => (row(0).toString, row(2).toString)}
	}


	def UserActionWithQueryAndAd(): RDD[(String, String, String)] = {
		RawUserAction().filter{row =>
						row(1) != null &&
						row(1).toString.indexOf("query=") >= 0 &&
						row(2).toString != "0" &&
						row(1).toString.split(",").filter(_.startsWith("query")).length > 0}
							.map{ row => (row(0).toString,
									row(1).toString.split(",").filter(_.startsWith("query="))(0).substring(6),
										row(2).toString)}
								.filter(_._2.length > 0)

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
			"    and (platform = 'wap' or platform = 'web')\n"
	}



	// seo interface
	def SeoWithAd() : RDD[(String, String)] = {
		RawUserAction().filter{row => row(2).toString != 0}
			.map{ row => (row(0).toString, row(2).toString)}
	}

	def SeoWithQuery() : RDD[String] = {
		RawSeo().filter{ row =>
			row(1) != null && row(1).toString.length != 0}
			.map{row => row(1).toString}
	}

	def SeoWithQueryAndAd() : RDD[(String, String, String)] = {
		RawSeo().filter{ row =>
			row(2) == null && row(2).toString != "0" && row(1) != null && row(1).toString.length != 0}
					.map{row => (row(0).toString, row(1).toString, row(2).toString)}
	}


	def RawSeo() : SchemaRDD = {
		Env.hiveContext().sql(SeoQuery())
	}

	def SeoQuery() : String ={
		"\nSELECT\n" +
			"   visitor_id,\n" +
			"   referer['query_word'],\n" +
			"   landing['ad_id']\n" +
			"FROM\n" +
			"   base.user_actions\n" +
			"WHERE\n" +
			"   dt between '" + Env.getProperty("fromdate") + "' and '" + Env.getProperty("todate") + "'\n" +
			"   and visitor['session_top_source'] = 'SEO'\n" +
			"   and landing['session_page_depth'] = '1'\n" +
			"   and landing['category_name_en'] = '" +  Env.getProperty("category")+ "'\n" +
			"   and platform in ('wap','web')\n" +
			"   and landing['city_id'] = '" + Env.getProperty("area_id") + "'"
	}



	// interface for tag and tag alias
	def allTag() : RDD[String] = {
		Env.sparkContext().textFile("/user/tianxing/allTag.csv").map((_, null)).sortByKey().keys
	}

	def tagQuery() : String = {
		"\nSELECT\n" +
		"   name,\n" +
		"   dimension_id\n" +
		"FROM\n" +
		"   logs.tagn"
	}

	def tagAliasQuery() : String = {
		"\nSELECT\n" +
		"   body\n" +
		"FROM\n" +
		"   logs.tag_alias"
	}


	def fangTagWithDimensionId() : RDD[(String, String)] = {
		Env.hiveContext().sql(fangTagQuery()).map{ row => (row(0).toString, row(1).toString)}
	}

	def fangTag() : RDD[String] = {
		Env.hiveContext().sql(fangTagQuery).map{row => row(0).toString}.map((_, null)).sortByKey().keys
	}

	def fangTagQuery() : String = {
		"\nSELECT\n" +
		"   name,\n" +
		"   dimension_id\n" +
		"FROM\n" +
		"   logs.tagn\n" +
		"WHERE\n" +
		"   category like '%fang%'\n" +
		"and \n" +
		"   area like '%m30'\n"
	}

	// interface for geli
	def gelis() : RDD[String] = {
		Env.sparkContext().textFile("/user/tianxing/geli/all/" + Env)
	}

}

