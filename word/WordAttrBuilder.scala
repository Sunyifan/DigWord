package word

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by abzyme-baixing on 14-11-11.
 */
object WordAttrBuilder {
	def wordInAdCount(inputRDD : RDD[(String, String)], wordList : Array[String]): RDD[(String, String)] = {
		inputRDD.flatMap{
			item : (String, String)=>

			val res = new ArrayBuffer[(String, String)]()
			for (word <- wordList){
				if (item._2.indexOf(word) >= 0){
					res += ((word, item._1))
				}
			}

			res.distinct
		}.reduceByKey(_ + "|" + _)
	}
}
