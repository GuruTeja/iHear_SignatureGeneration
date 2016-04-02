package org.altic.spark.clustering

import org.altic.spark.clustering.utils.SparkReader
import org.apache.spark.SparkContext

/**
 * Created by pradyumnad on 3/18/16.
 */
object ClassSplitterApp {

  def context(): SparkContext = {
    val prgName = this.getClass.getSimpleName
    println("## LOCAL ##")
    new SparkContext("local", prgName)
  }

  private def parseSIFTFeatureLine(line: String): (String, String) = {
    val parts = line.split(',')
    val feature = parts(parts.length - 1).trim
    val filepath = parts(0).split("/")
    // print(feature)

    val features = feature.split(' ').map(_.trim.toDouble)
    val label = filepath(filepath.length - 2)
    (label, line)
  }

  def main(args: Array[String]): Unit = {
    val sc = context()
    println(sc.version)

    val filePath = "/Users/pradyumnad/Documents/Generated/6_ObjectCategories/SIFT10"
    val data = sc.textFile(filePath).filter(_.length > 1).map(parseSIFTFeatureLine).groupByKey().reduceByKey((a, b) => a++b)
    data.saveAsTextFile(filePath+"/Group")

//    println(data.take(1).toList)
//    data.foreach
//    {
//      case i =>
//        sc.parallelize(i._2.toSeq).coalesce(1).saveAsTextFile(filePath+"/class_"+i._1)
//    }
  }
}