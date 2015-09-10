

/*
 A very basic implementation of Finite Element Analysis using Spark
 */
package fourquant

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

/** A collection of graph generating functions. */
object FEMDemo {

  def main(args: Array[String]): Unit = {
    import fourquant.FEM._
    val p = SparkGlobal.activeParser(args)
    val imSize = p.getOptionInt("size", 50,
      "Size of the image to run the test with");
    val easyRow = Array(0, 1, 1, 0, 1, 4, 2, 1, 3, 1, 2, 3, 3, 4, 3, 2, 1, 2, 3)
    val testImg = Array(easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow);
    val sc = SparkGlobal.getContext()
    val myGraph = twoDArrayToGraph(sc, testImg)
    myGraph.triplets.
      map(triplet => triplet.srcAttr + " is connected to  " + triplet.dstAttr + " via " + triplet
      .attr).
      collect.foreach(println(_))

    val out = calcForces(myGraph)

    out.triplets.
      map(triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr).
      collect.foreach(println(_))

    sumForces(out).
      foreach(cpt => println(cpt._2._2.pos.toString + ": " + cpt._2._1))

  }

}

class FEMTests {
    
}