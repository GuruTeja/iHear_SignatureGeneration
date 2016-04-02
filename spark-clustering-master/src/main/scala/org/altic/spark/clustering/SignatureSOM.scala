package org.altic.spark.clustering

import java.io.File

import org.altic.spark.clustering.som.SomTrainerB
import org.altic.spark.clustering.utils.{IO, SparkReader}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Vector

/**
 * Created by pradyumnad on 3/4/16.
 */
object SignatureSOM {
  def context(): SparkContext = {
    val prgName = this.getClass.getSimpleName
    println("## LOCAL ##")
    new SparkContext("local", prgName)
  }

  class Experience(val datasetDir: String, val name: String, val nbProtoRow: Int, val nbProtoCol: Int, val nbIter: Int, val ext: String = ".data") {
    def dir: String = datasetDir + "/" + name

    def dataPath: String = if(ext.length > 1) dir + "/" + name + ext else dir

    def nbTotProto: Int = nbProtoCol * nbProtoRow
  }

  def cleanResults(exp: Experience) {
    val resultDirs = new File(exp.dir).listFiles.filter(_.getName.startsWith(exp.name + ".clustering"))
    resultDirs.foreach(IO.delete)
    println("Removed " + exp.dir)
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName).listFiles.filter(_.isDirectory).map(_.getName)
  }

  def main(args: Array[String]) {
    //    PropertyConfigurator.configure("log4j.properties")
    val sc = context()

    var globalDatasetDir = "./data"
    // Magi Cluster mode
    if (System.getenv("SPARK_CLUSTERING") != null) {
      globalDatasetDir = System.getenv("SPARK_CLUSTERING") + "/data"
    }

    val globalNbIter = 10
    val nbExp = 1

    // Note ; map height/width = round(((5*dlen)^0.54321)^0.5)
    //        dlen is the number of row of the dataset

    val filepath = "/Users/mayanka/Desktop/GuRu/MaStErS/ThEsIs/sErVeR_SiDe/SoM_TestData"
    val dirs = getListOfSubDirectories(filepath)
    val experiences = dirs.map(d => new Experience(filepath, d, 1, 10, globalNbIter, ext = ""))
    println("experiences is: "+experiences)

    //    val experiences = Array(
    //      new Experience(globalDatasetDir, "Glass", 1, 7, globalNbIter)
    //       new Experience("/Users/pradyumnad/Documents/Generated/6_ObjectCategories/", "SIFT10", 1, 10, globalNbIter, ext = ""),
    //       new Experience("/Users/pradyumnad/Documents/Generated/6_ObjectCategories_New/", "SIFT50", 1, 50, globalNbIter, ext = "")
    //    )

    experiences.foreach { exp =>
      // Delete old results
      cleanResults(exp)

      // Read data from fil
      println("\n***** READ : " + exp.name)
      val datas = SparkReader.parseSIFT(sc, exp.dataPath).cache()

      for (numExp <- 1 to nbExp) {
        println("\n***** SOM : " + exp.name + " (" + numExp + "/" + nbExp + ")")
        val som = new SomTrainerB
        val somOptions = Map("clustering.som.nbrow" -> exp.nbProtoRow.toString, "clustering.som.nbcol" -> exp.nbProtoCol.toString)
        val somConvergeDist = -0.1
        som.training(datas.asInstanceOf[RDD[Vector]], somOptions, exp.nbIter, somConvergeDist)

        // Save results
        som.associations(datas).map(d => d._1 + "," + d._2 + " - " + d._3)
          .saveAsTextFile(exp.dir + "/" + exp.name + ".clustering.som_" + exp.nbProtoRow + "x" + exp.nbProtoCol + "-" + numExp)
        som.signature(sc, datas)
          //          .map(d => d._1 + "," + d._2)
          .map(d => d._1)
          .saveAsTextFile(exp.dir + "/" + exp.name + ".clustering.som_centers_" + exp.nbProtoRow + "x" + exp.nbProtoCol + "-" + numExp)
      }

      //      kmeansSignature(sc, globalNbIter, exp)
    }
  }

  def kmeansSignature(sc: SparkContext, globalNbIter: Int, exp: Experience): Unit = {
    val dataKM = SparkReader.parseForKMeans(sc, exp.dataPath).cache()
    // Cluster the data into two classes using KMeans
    // Load and parse the data
    val clusters = KMeans.train(dataKM, exp.nbTotProto, globalNbIter)
    //      clusters.save(sc, exp.dir + "/" + exp.name)
    println(clusters.clusterCenters.mkString(" "))
    val kMeansCenters = sc.parallelize(clusters.clusterCenters)
    kMeansCenters.saveAsTextFile(exp.dir + "/" + exp.name + ".clustering.kmeans_centers_" + exp.nbTotProto)
    val preds = dataKM.map(p => p.toArray.mkString(" ") + "," + clusters.predict(p))
    preds.saveAsTextFile(exp.dir + "/" + exp.name + ".clustering.kmeans_" + exp.nbTotProto)
  }
}
