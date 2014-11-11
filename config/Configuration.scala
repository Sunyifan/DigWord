package config

/**
 * Created by abzyme-baixing on 14-11-7.
 */
class Configuration {
	private var properties = Map[String, String]()

	def this(area_id : String, category : String, fromdate : String, todate : String) = {
		this()
		this.set("area_id", area_id)
		this.set("category", category)
		this.set("fromdate", fromdate)
		this.set("todate", todate)
	}

	def get(prop : String): String = properties(prop)
	def set(prop : String, value : String) : Unit = properties += (prop -> value)
	override def toString() = properties("area_id") + "_" + properties("category") +
										"_" + properties("fromdate") + "_" + properties("todate")
}
