package config

/**
 * Created by abzyme-baixing on 14-11-7.
 */
object Configuration {
	var properties = Map[String, String]()

	def get(prop : String): String = properties(prop)
	def set(prop : String, value : String) : Unit = properties += (prop -> value)

	def main(args : Array[String]): Unit ={
		set("abc", "efg")
		println(get("abc"))
	}
}
