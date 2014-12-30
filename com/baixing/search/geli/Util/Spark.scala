package com.baixing.search.geli.Util

import org.apache.log4j.{Logger, Level}

/**
 * Created by abzyme-baixing on 14-12-23.
 */
object Spark {
	def silenceSpark() {
		setLogLevels(Level.OFF, Seq("org", "spark", "org.eclipse.jetty", "akka"))
	}

	def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
		loggers.map{
			loggerName =>
				val logger = Logger.getLogger(loggerName)
				val prevLevel = logger.getLevel()
				logger.setLevel(level)
				loggerName -> prevLevel
		}.toMap
	}

}
