package com.baixing.search.geli.Util

import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD

/**
 * Created by abzyme-baixing on 14-11-27.
 */
object Data {
	def adContent() : RDD[String] ={
		DataSource.getRaw("logs.ad_content", Array("title", "content", "dt", "area_id", "category"))
					.filter(row => row(3).toString == Env.sparkConf().get("area_id"))
						.filter(row => row(4).toString == Env.sparkConf().get("category"))
							.filter(row => row(2).toString >= Env.sparkConf().get("fromdate") && row(2).toString <= Env.sparkConf().get("todate"))
								.map(row => row(0).toString + " " + row(1).toString)
	}

	def tag() : RDD[String] = {
		DataSource.getRaw("logs.tagn", Array("name")).map(row => row(0).toString)
	}
}

object DataSource{
	def getRaw(tableName : String, fields : Array[String]): SchemaRDD ={
		val hc = Env.hiveContext()
		import hc._

		hc.table(tableName).select(fields.map(field => symbolToUnresolvedAttribute(Symbol(field))):_*)
	}
}
