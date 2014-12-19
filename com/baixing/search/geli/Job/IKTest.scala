package com.baixing.search.geli.Job

import java.io.StringReader

import com.baixing.search.geli.Environment.Env
import com.baixing.search.geli.Util.{Text, Data}
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.dic.Dictionary
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
 * Created by abzyme-baixing on 14-12-17.
 */
object IKTest {
	def main(args : Array[String]): Unit ={
		Env.init(args)

		val ads = Data.adContentWithId().repartition(Env.getProperty("partition").toInt)

		val gelis = Env.sparkContext().textFile("/user/sunyifan/geli/all/" + Env.output()).map{
			line =>
				line.split(",").head
		}.collect

		val bgelis = Env.sparkContext().broadcast(gelis)

		ads.map{
			ad : (String, String) =>
				(ad._1, Text.preproccess(ad._2).mkString(" "))
		}.map{
			ad =>
				val segmenter = new IKSegmenter(new StringReader(ad._2), true)
				val dictionary = Dictionary.getSingleton
				dictionary.addWords(JavaConversions.asJavaCollection(bgelis.value))

				val loop = new Breaks
				val splitedWords = new ArrayBuffer[String]

				loop.breakable{
					while(true){
						val lex = segmenter.next()
						if(lex == null)
							loop.break
						splitedWords += lex.getLexemeText
					}
				}

				(ad._1, splitedWords.mkString("@"))
		}.saveAsTextFile("/user/sunyifan/tmp")
	}
}
