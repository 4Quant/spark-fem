

/*
 A very basic implementation of Finite Element Analysis using Spark
 */
package fourquant

import org.scalatest.{FunSuite, Matchers}

class FEMTests extends FunSuite with Matchers with LocalSparkContext with Serializable {

  val easyRow = Array(0, 1, 1, 0, 1, 4, 2, 1, 3, 1, 2, 3, 3, 4, 3, 2, 1, 2, 3)
  val testImg = Array(easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow, easyRow);

  val dt = 0.01
  val steps = 5
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
      foreach(cpt => println(cpt._2._2.real_pos.toString + ": " + cpt._2._1))

  }

  test("Move a point along") {
    val sc = getSpark("Operate Graph")
    val myGraph = fem.functions.twoDArrayToGraph(sc, testImg)
    val firstForces = fem.functions.calcForces(myGraph)
    val summedForces = fem.functions.sumForces(firstForces)

    val newGraph = fem.functions.movePoints(firstForces,dt)
    newGraph.vertices.collect.
      foreach { cpt =>
      println(cpt._2.real_pos.toString + ": " + cpt._1)
      assert(cpt._2.get_history().length == 1, "History should be length 1 at every point")
    }

  }

  test("Move a few steps") {
    val sc = getSpark("Operate Graph")
    val myGraph = fem.functions.twoDArrayToGraph(sc, testImg)
    val movedGraph = fem.functions.nsteps(myGraph,dt,steps)
    movedGraph.vertices.collect.foreach{
      case (id,iv) =>
        println(s"""Point:$id\t->${iv.get_history().mkString(",")}""")
        assert(iv.get_history().length == 5, "History should be length 5 at every point")
    }
  }

  test("Transform to a list of points") {
    val sc = getSpark("Transform Graph")
    val myGraph = fem.functions.twoDArrayToGraph(sc, testImg)

    val movedGraph = fem.functions.nsteps(myGraph,dt,steps)
    val ptList = fem.functions.expandMovingPoints(movedGraph,dt)
    ptList.groupBy(_.index).collect.foreach{
      case(id,ptGrp) =>
        assert(ptGrp.toList.length == 5, "There should be 5 for each index")
    }
    ptList.map(_.time).min should be >= 0.0
    ptList.map(_.time).max should be < ((steps+1)*dt)

  }
}