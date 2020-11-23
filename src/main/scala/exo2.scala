package com.fakir.samples
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object exo2 {

  //EXO 2
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //question 1
    val df_film = sparkSession.read.option("delimiter", ";").option("inferSchema", true).option("header", false).csv("data/donnees.csv")

    //question 2
    val df_film_rename = df_film.withColumnRenamed(existingName = "_c0", newName = "nom_film").withColumnRenamed(existingName = "_c1", newName = "nombre_vues").withColumnRenamed(existingName = "_c2", newName = "note_film").withColumnRenamed(existingName = "_c3", newName = "acteur_principal")
    df_film_rename.show

    //question 3
    //Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val DiCap = df_film_rename.filter(col("acteur_principal") === "Di Caprio").count()
    println(DiCap)

    //Quelle est la moyenne des notes des films de Di Caprio ?
    val average_DiCap = df_film_rename.groupBy("acteur_principal").mean("note_film").filter(col("acteur_principal") === "Di Caprio")
    average_DiCap.show

    //Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?

    //question 4
    val total_vue = df_film_rename.groupBy("nombre_vues").sum()
    df_film_rename.withColumn("pourcentage de vues", (col("nombre_vues")/ total_vue)*100).show()

  }

}
