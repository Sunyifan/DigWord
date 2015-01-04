package com.baixing.search.geli.Data

/**
 * Created by abzyme-baixing on 15-1-4.
 */
abstract class BaseQuery {
	abstract def accept() : Boolean
}

class Query(field : String, value : Any) extends BaseQuery{
	override def accept() : Boolean ={
		value match {
			case String => field == value.toString
		}
	}
}



