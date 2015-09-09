

/*
 A very basic implementation of Finite Element Analysis using Spark
 */
package tipl.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.rdd.RDD
import tipl.util.D3float
import tipl.util.D3int
import tipl.spark.IOOps._
import tipl.util.TIPLOps._
import org.apache.spark.SparkContext._

/** A collection of graph generating functions. */
object FEMDemo {


  /**
   * A class for storing the image vertex information to prevent excessive tuple-dependence
   */
  case class ImageVertex(index: Int, pos: D3int = new D3int(0), value: Int = 0,
                                       original: Boolean = false) extends Serializable


  /**
   * Edges connecting images, the orientation is the unit vector between the two points
   *
   **/
  case class ImageEdge(dist: Double, orientation: D3float, restLength: Double = 1.0)
    extends Serializable


  case class ForceEdge(ie: ImageEdge, force: D3float) extends Serializable


  implicit class ieSub(iv: ImageVertex) extends Serializable {
    def -(iv2: ImageVertex): ImageEdge = {
      val xd = iv.pos.x - iv2.pos.x
      val yd = iv.pos.y - iv2.pos.y
      val zd = iv.pos.z - iv2.pos.z
      val bDist = Math.sqrt(Math.pow(xd, 2) + Math.pow(yd, 2) + Math.pow(zd, 2))
      new ImageEdge(bDist, new D3float(xd / bDist, yd / bDist, zd / bDist), 1)
    }
  }


  val extractPoint = (idx: Int, inArr: Array[Array[Int]], xwidth: Int, ywidth: Int) => {
    val i = Math.floor(idx * 1f / xwidth).toInt
    val j = idx % xwidth
    new ImageVertex(idx, new D3int(i, j, 0), inArr(i)(j), true)
  }

  def spreadVertices(pvec: ImageVertex, windSize: Int = 1) = {
    val wind = (0 to windSize)
    val pos = pvec.pos
    val z = 0
    for (x <- wind; y <- wind)
    yield new ImageVertex(pvec.index, new D3int(pos.x + x, pos.y + y, pos.z + z), pvec.value,
      (x == 0) & (y == 0) & (z == 0))
  }

  def twoDArrayToGraph(sc: SparkContext, inArr: Array[Array[Int]]): Graph[ImageVertex,
    ImageEdge] = {
    val ywidth = inArr.length
    val xwidth = inArr(0).length
    val vertices = sc.parallelize(0 until xwidth * ywidth).map {
      idx => extractPoint(idx, inArr, xwidth, ywidth)
    }
    val fvertices: RDD[(VertexId, ImageVertex)] = vertices.map(cpt => (cpt.index, cpt))
    val edges = vertices.flatMap {
      cpt => spreadVertices(cpt, 1)
    }.groupBy(_.pos).filter {
      // at least one original point
      ptList => ptList._2.map(_.original).reduce(_ || _)
    }.flatMap {
      combPoint => {
        val pointList = combPoint._2
        val centralPoint = pointList.filter(_.original).head
        val neighborPoints = pointList.filter(pvec => !pvec.original)
        for (cNeighbor <- neighborPoints)
        yield Edge[Unit](centralPoint.index, cNeighbor.index)
      }

    }

    Graph[ImageVertex, Unit](fvertices, edges).
      mapTriplets(triplet => triplet.srcAttr - triplet.dstAttr)
  }

  def calcForces(inGraph: Graph[ImageVertex, ImageEdge]) = {
    inGraph.mapEdges(
      rawEdge => {
        val edge: ImageEdge = rawEdge.attr
        val k = 0.01
        val force = (edge.restLength - edge.dist)
        new ForceEdge(edge, edge.orientation * force)
      })
  }

  def sumForces(mGraph: Graph[ImageVertex, ForceEdge]) = {
    mGraph.mapReduceTriplets[D3float](
      // map function
      triplet => {
        Iterator((triplet.srcId, triplet.attr.force),
          (triplet.dstId, triplet.attr.force * (-1))
        )
      },
      // reduce function
      (force1: D3float, force2: D3float) => force1 + force2
    ).join(mGraph.vertices)
  }

  def main(args: Array[String]): Unit = {
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