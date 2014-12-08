package com.baixing.search.geli.Util

import org.apache.spark.rdd.RDD

/**
 * Created by abzyme-baixing on 14-12-5.
 */


// Ad相关的数据预处理层，预处理原始数据，给出计算输入
object AdConverter {
	// in : [ad_id, ad_title, ad_content, tags : string]
	private val rawAd = DataSource.rawAd().persist()

	//out : [ad_id, tags : array]
	def adTag(): RDD[(String, Array[String])] ={
		rawAd.filter{row => row(3) != null}.map{row => (row(0).toString, row(3).toString.split("@"))}
	}

	def adContent() : RDD[String] = {
		rawAd.filter{row => row(1) != null && row(2) != null}.map{row => row(1).toString + "  " + row(2).toString}
	}
}

// UserAction相关的数据预处理
object UserActionConverter {
	// in : [visitorid, visitor, referer, landing]
	private val rawUserAction = DataSource.rawUserAction().persist()

	// 站内搜索串的转化
	def baixingQuery(): RDD[(String, String)] ={
		rawUserAction.filter{row => row(2) != null}
						.map{row => (row(0),row(2))}
							.map{case (visitorid : String, referer : Map[String, String]) => (visitorid, referer("query"))}
								.filter(_._2.indexOf("query=") >= 0)
									.map{ item => (item._1, item._2.split(",").filter(_.startsWith("query=")).head.substring(6))}
	}

	def baixingQueryAndAd() : RDD[(String, String, String)] = {
		rawUserAction.filter{row => row(2) != null && row(3) != null}
						.map{row => (row(0), row(2), row(3))}
							.map{case (visitorid : String, referer : Map[String, String], landing : Map[String, String])
									=> (visitorid, referer("query"), landing("ad_id"))}
								.filter(_._2.indexOf("query=") >= 0)
									.filter(_._3 != "0")
	}

	// seo搜索串的转化
	def seoQuery() : RDD[(String, String)] = {
		rawUserAction.filter{row => row(1) != null && row(2) != null}
						.map{row => (row(0),row(1),row(2), row(3))}
							.filter{case (visitorid : String, visitor : Map[String, String], referer : Map[String, String], landing : Map[String, String])
										=> visitor("session_top_source") == "SEO" && landing("session_page_depth") == "1"}
								.map{case (visitorid : String, visitor : Map[String, String], referer : Map[String, String], landing : Map[String, String])
										=> (visitorid, referer("query_word"))}
	}

	def seoQueryAndAd() : RDD[(String, String, String)] = {
		rawUserAction.filter{row => row(1) != null && row(2) != null && row(3) != null}
						.map{row => (row(0),row(1),row(2), row(3))}
							.filter{case (visitorid : String, visitor : Map[String, String], referer : Map[String, String], landing : Map[String, String])
									=> visitor("session_top_source") == "SEO" && landing("session_page_depth") == "1" && landing("ad_id") != "0"}
								.map{case (visitorid : String, visitor : Map[String, String], referer : Map[String, String], landing : Map[String, String])
									=> (visitorid, referer("query_word"), landing("ad_id"))}
	}
}
