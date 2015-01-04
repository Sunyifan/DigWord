package com.baixing.search.geli.Util

import com.baixing.search.geli.Environment.Env
import org.apache.spark.sql.SchemaRDD

/**
 * Created by abzyme-baixing on 14-11-27.
 */
object Data {
	def getRaw(tableName : String, fields : Array[String]): SchemaRDD ={
		val hc = Env.hiveContext()
		import hc._

		hc.table(tableName).select(fields.map(field => symbolToUnresolvedAttribute(Symbol(field))):_*)

	}
}
