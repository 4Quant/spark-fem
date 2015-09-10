package fourquant.fem

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
* The standard functions needed for FEM analysis
*/

object functions {
  val extractPoint = (idx: Int, inArr: Array[Array[Int]], xwidth: Int, ywidth: Int) => {
    val i = Math.floor(idx * 1f / xwidth).toInt
    val j = idx % xwidth
    types.ImageVertex(idx, types.D3int(i, j, 0), inArr(i)(j), true)
  }
  def spreadVertices(pvec: types.ImageVertex, windSize: Int = 1) = {
    val wind = (0 to windSize)
    val pos = pvec.pos
    val z = 0
    for (x <- wind; y <- wind)
      yield  types.ImageVertex(pvec.index, new types.D3int(pos.x + x, pos.y + y, pos.z + z),
        pvec.value, (x == 0) & (y == 0) & (z == 0))
  }

  def twoDArrayToGraph(sc: SparkContext, inArr: Array[Array[Int]]):
  Graph[types.ImageVertex, types.ImageEdge] = {
    val ywidth = inArr.length
    val xwidth = inArr(0).length
    val vertices = sc.parallelize(0 until xwidth * ywidth).map {
      idx => extractPoint(idx, inArr, xwidth, ywidth)
    }

    val fvertices: RDD[(VertexId, types.ImageVertex)] = vertices.map(cpt => (cpt.index, cpt))

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

    Graph[types.ImageVertex, Unit](fvertices, edges).
      mapTriplets(triplet => triplet.srcAttr - triplet.dstAttr)
  }
  def calcForces(inGraph: Graph[types.ImageVertex, types.ImageEdge]) = {
    inGraph.mapEdges(
      rawEdge => {
        val edge: types.ImageEdge = rawEdge.attr
        val k = 0.01
        val force = (edge.restLength - edge.dist)
        types.ForceEdge(edge, edge.orientation * force)
      })
  }

  def sumForces(mGraph: Graph[types.ImageVertex, types.ForceEdge]) = {
    mGraph.mapReduceTriplets[types.D3float](
      // map function
      triplet => {
        Iterator((triplet.srcId, triplet.attr.force),
          (triplet.dstId, triplet.attr.force * (-1))
        )
      },
      // reduce function
      (force1: types.D3float, force2: types.D3float) => force1 + force2
    ).join(mGraph.vertices)
  }
}
