package util

import config.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.SparkContext
import environ.Env


/**
 * Created by abzyme-baixing on 14-11-7.
 */
object DataStorage {
	def buildInputRDD(env : Env,  conf : Configuration): RDD[(String, String)] ={
		find(env.getSparkContext(), conf.get("area_id"), conf.get("category"), conf.get("fromdate"), conf.get("todate"))
			.map{item  => (item(0).toString, item(1).toString + " " + item(2).toString)}
	}

	def insertWordInAdRDD(wordInAdIdRDD : RDD[(String, String)], env : Env, conf : Configuration) : Unit = {
		for (word <- wordInAdIdRDD.collect){
			insert(env.getSparkContext(), word._1, word._2, conf.get("area_id"), conf.get("category"),
				conf.get("fromdate"), conf.get("todate"))
		}
	}

	def find(sc : SparkContext, areaid : String, category : String, fromdate : String, todate : String) : SchemaRDD = {
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

		hiveContext.hql(getQuery(areaid, category, fromdate, todate))
	}

	def insert(sc : SparkContext, word : String, adIds : String, area_id : String, category : String,
		                    fromdate : String, todate : String, source : String = "Ad"): SchemaRDD ={
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

		hiveContext.hql(insertQuery(word, adIds, area_id, category, fromdate, todate, source))
	}

	def getQuery(areaid : String, category : String, fromdate : String, todate: String): String ={
		" SELECT ad_id, title, content"  +
		" FROM shots.ad_content " +
		" WHERE category = '" + category + "' and " +
				"area_id = '" + areaid + "' and " +
				"dt < '" + todate + "' and " +
				"dt >= '" + fromdate + "'"
	}

	def insertQuery(word : String, adIds : String, area_id : String, category : String,
	                                fromdate : String, todate : String, source : String): String ={
		" INSERT INTO TABLE hivetemp.geliword" +
		" (word, adids, area_id, category, fromdate, todate, source)" +
		" values ('" + word + "','" + adIds + "','" + area_id + "','" +
						category + "','" + fromdate + "','" + todate +
						"','" + source + "')"
	}

}
