package com.baixing.search.geli.Runner

import com.baixing.search.geli.Configuration.Configuration
import com.baixing.search.geli.Environment.Env
import org.apache.spark.SparkContext._

/**
 * Created by abzyme-baixing on 14-11-12.
 */
object WordRelationByUserAction {
	def main(args : Array[String]): Unit ={
		val conf = new Configuration(args(0), args(1), args(2), args(3))
		val env = new Env(conf)

		val pearlUV = UserActionCalculator.pearlUV(conf, env)
		val geli2pearlUV = UserActionCalculator.geli2pearlUV(conf, env)


		geli2pearlUV.map{
			item : ((String, String), Int) =>
				(item._1._1, (item._1._2, item._2))
		}.join(pearlUV).map{
			item : (String, ((String, Int), Int)) =>
				((item._1, item._2._1._1), item._2._1._2.toDouble / item._2._2.toDouble)
		}.saveAsTextFile(args(4))
	}
}
