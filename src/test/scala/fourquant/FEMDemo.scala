

/*
 A very basic implementation of Finite Element Analysis using Spark
 */
package fourquant

import org.scalatest.{FunSuite, Matchers}

class FEMTests extends FunSuite with Matchers with LocalSparkContext {

  val easyRow = Array(0, 1, 1, 0, 1, 4, 2, 1, 3, 1, 2, 3, 3, 4, 3, 2, 1, 2, 3)
  val testImg = Array(easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow);

  test("Create Graph from Points") {
      val sc = getSpark("Create Graph")
      val myGraph = fem.functions.twoDArrayToGraph(sc, testImg)
      myGraph.triplets.
      map(triplet => triplet.srcAttr + " is connected to  " + triplet.dstAttr + " via " + triplet
      .attr).
      collect.foreach(println(_))
  }

  test("Operate on Graph") {
    val sc = getSpark("Operate Graph")
    val myGraph = fem.functions.twoDArrayToGraph(sc, testImg)
    val out = fem.functions.calcForces(myGraph)

    out.triplets.
      map(triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr).
      collect.foreach(println(_))

    fem.functions.sumForces(out).
      foreach(cpt => println(cpt._2._2.pos.toString + ": " + cpt._2._1))

  }
}