package util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by abzyme-baixing on 14-11-7.
 */
object DataStorage {
	def find(sc : SparkContext, fields : Array[String],  category : String, areaid : String, date : String) : RDD[(String, String)] = {
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

		hiveContext.hql(getQuery(fields, category, areaid, date)).map(
							item => (item(0).toString, item(1).toString + " " + item(2).toString))
	}

	def getQuery(fields : Array[String],  category : String, areaid : String, date : String): String ={
		" SELECT " + fields.mkString(",") +
		" FROM shots.ad_content " +
		" WHERE category = '" + category + "' and " +
				"area_id = '" + areaid + "' and " +
				"dt = '" + date + "'"
	}
}
