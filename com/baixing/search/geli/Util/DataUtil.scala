package com.baixing.search.geli.Util

import com.baixing.search.geli.Configuration.Configuration
import com.baixing.search.geli.Environment.Env
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD


/**
 * Created by abzyme-baixing on 14-11-12.
 */
object Data {
	def AdInputRDD(conf : Configuration, env : Env) : RDD[(String,String)] = {
		Ad(conf, env).map{ row => (row(0).toString, row(1).toString + " " + row(2).toString)}
	}

	private def Ad (conf : Configuration, env : Env) : SchemaRDD = {
		env.hiveContext().hql(AdQuery(conf))
	}

	private def AdQuery (conf : Configuration) : String = {
		val category = conf.category
		val areaid = conf.areaId
		val fromdate = conf.fromdate
		val todate = conf.todate

		" SELECT ad_id, title, content"  +
			" FROM shots.ad_content " +
			" WHERE category = '" + category + "' and " +
			"area_id = '" + areaid + "' and " +
			"dt < '" + todate + "' and " +
			"dt >= '" + fromdate + "'"
	}
}
