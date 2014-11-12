package com.baixing.search.geli.Configuration

import scala.collection.mutable.HashMap

/**
 * Created by abzyme-baixing on 14-11-12.
 */
class Configuration {
	private val properties = HashMap[String, String]()

	def this(area_id : String, category : String, fromdate : String, todate : String, mergeDate : Boolean = false) = {
		this()
		this.set("area_id", area_id)
		this.set("category", category)
		this.set("fromdate", fromdate)
		this.set("todate", todate)
		this.set("mergeDate", mergeDate.toString)
	}

	private def get(prop : String): String = properties(prop)
	private def set(prop : String, value : String) : Unit = properties += (prop -> value)
	def areaId() : String = get("area_id")
	def category() : String = get("category")
	def fromdate() : String = get("fromdate")
	def todate() : String = get("todate")

	override def toString() = properties("area_id") + "_" + properties("category") +
		"_" + properties("fromdate") + "_" + properties("todate")

}
