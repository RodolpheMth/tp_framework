package com.fakir.samples
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.col

object SampleProgram {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //question 2
    val findDiCapRDD = rdd.filter(elem => elem.contains("Di Caprio"))
    val nb_film = findDiCapRDD.count()
    println(nb_film)

    //question 3
    val notations = findDiCapRDD.map(item => (item.split(";")(2).toDouble))
    val average = notations.sum()/notations.count()
    println(average)

    //question 4
    val total = rdd.map(item => (item.split(";")(1).toDouble))
    val vues = findDiCapRDD.map(item => (item.split(";")(1).toDouble))
    val pourcentage_echantillon = vues.sum() / total.sum()
    println(pourcentage_echantillon)

    //question 5


  }


}


