

/*
The basic types used for FEM analysis, these can be replaced with other types if needed for more
complicated cases
 */
package fourquant.fem

object types extends Serializable {
    case class D3float(x: Double, y: Double, z: Double) {
      def *(df: D3float) = D3float(x*df.x,y*df.y,z*df.z)
      def *(dv: Double) = D3float(x*dv,y*dv,z*dv)
      def +(df: D3float) = D3float(x+df.x,y+df.y,z+df.z)
    }
    case class D3int(x: Int=0, y: Int=0, z: Int=0) {
      def toF() = D3float(x,y,z)
    }
    /**
     * A class for storing the image vertex information to prevent excessive tuple-dependence
     */
    case class ImageVertex(index: Int, pos: D3int = D3int(0,0,0), value: Int = 0,
                           original: Boolean = false)


    /**
     * Edges connecting images, the orientation is the unit vector between the two points
     *
     **/
    case class ImageEdge(dist: Double, orientation: D3float, restLength: Double = 1.0)

    /**
     * An edge carrying a specific force

     */
    case class ForceEdge(ie: ImageEdge, force: D3float)


    /**
     * Implements subtraction for image vertices
     * @param iv
     */
    implicit class ieSub(iv: ImageVertex) extends Serializable {
      def -(iv2: ImageVertex): ImageEdge = {
        val xd = iv.pos.x - iv2.pos.x
        val yd = iv.pos.y - iv2.pos.y
        val zd = iv.pos.z - iv2.pos.z
        val bDist = Math.sqrt(Math.pow(xd, 2) + Math.pow(yd, 2) + Math.pow(zd, 2))
        new ImageEdge(bDist, types.D3float(xd / bDist, yd / bDist, zd / bDist), 1)
      }
    }
  }