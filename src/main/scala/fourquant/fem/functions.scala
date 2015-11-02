package fourquant.fem

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
* The standard functions needed for FEM analysis
*/

object functions extends Serializable {

  def extractPoint[T](idx: Int, inArr: Array[Array[T]], xwidth: Int, ywidth: Int) = {
    val i = Math.floor(idx * 1f / xwidth).toInt
    val j = idx % xwidth
    types.SimpleImageVertex[T](idx, types.D3int(i, j, 0), inArr(i)(j), true)
  }

  def spreadVertices[T](pvec: types.ImageVertex[T], windSize: Int = 1) = {
    val wind = (0 to windSize)
    val pos = pvec.lat_pos
    val z = 0
    for (x <- wind; y <- wind)
      yield pvec.shift(x,y,0)
  }

  def twoDArrayToGraph[T](sc: SparkContext, inArr: Array[Array[T]]):
  Graph[types.ImageVertex[T], types.ImageEdge] = {
    val ywidth = inArr.length
    val xwidth = inArr(0).length
    val vertices = sc.parallelize(0 until xwidth * ywidth).map {
      idx => extractPoint(idx, inArr, xwidth, ywidth)
    }

    val fvertices: RDD[(VertexId, types.ImageVertex[T])] = vertices.map(cpt => (cpt.index, cpt))

    val edges = vertices.flatMap {
      cpt => spreadVertices(cpt, 1)
    }.groupBy(_.lat_pos).filter {
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

    Graph[types.ImageVertex[T], Unit](fvertices, edges).
      mapTriplets(triplet => triplet.srcAttr - triplet.dstAttr)
  }

  def calcForces[T](inGraph: Graph[types.ImageVertex[T], types.ImageEdge]) = {
    inGraph.mapEdges {
      rawEdge =>
        val edge: types.ImageEdge = rawEdge.attr
        val k = 0.01
        val force = (edge.restLength - edge.dist)
        types.ForceEdge(edge, edge.orientation * force)
    }
  }

  def sumForces[T](mGraph: Graph[types.ImageVertex[T], types.ForceEdge]) = {
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



  def movePoints[T](mGraph: Graph[types.ImageVertex[T], types.ForceEdge], dt: Double):
    Graph[types.MovingImageVertex[T],types.ForceEdge] = {
    val summedForce = sumForces(mGraph)
    val newEdges: RDD[(VertexId, types.MovingImageVertex[T])] = summedForce.map{
      case (vertex: VertexId, (newForce: types.D3float, iv: types.ImageVertex[T])) =>
        (vertex,
          iv.move(newForce*dt,dt) // the move command updates the velocity as well
          )
    }
    Graph(newEdges,mGraph.edges)
  }

  def nsteps[T](mGraph: Graph[types.ImageVertex[T], types.ImageEdge], dt: Double, n: Int):
    Graph[types.MovingImageVertex[T],types.ForceEdge] = {
    val fGraph = calcForces(mGraph)
    if (n>1) {
      val uGraph = movePoints(fGraph,dt).
        mapEdges {
        cur_e: Edge[types.ForceEdge] => cur_e.attr.ie
      }.mapVertices {
        (cur_id: VertexId, cur_v: types.MovingImageVertex[T]) =>
          cur_v.asIV
      }

      nsteps(uGraph, dt,n-1)
    } else { // it is 1 now
      movePoints(fGraph,dt)
    }
  }

}
