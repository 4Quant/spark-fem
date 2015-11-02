

/*
The basic types used for FEM analysis, these can be replaced with other types if needed for more
complicated cases
 */
package fourquant.fem

object types extends Serializable {
  case class D3float(x: Double, y: Double = 0 , z: Double = 0) {

    def *(df: D3float) = D3float(x*df.x,y*df.y,z*df.z)
    def *(dv: Double) = D3float(x*dv,y*dv,z*dv)
    def +(df: D3float) = D3float(x+df.x,y+df.y,z+df.z)
  }
  case class D3int(x: Int=0, y: Int=0, z: Int=0) {
    def toF() = D3float(x,y,z)
  }


  /**
   * A vertex point
   * @tparam T
   */
  trait ImageVertex[T] extends Serializable {
    val index: Long
    val lat_pos: D3int // inside the lattice (for spread events)
    val real_pos: Option[D3float] // the real-world position
    val value: T
    val original: Boolean

    /**
     * Shift a point along the lattice
     * @param dx x displacement
     * @param dy y displacement
     * @param dz z displacement
     * @return a new vertex at the position
     */
    def shift(dx: Int, dy: Int, dz: Int): ImageVertex[T]


    /**
     * Move a point along a given velocity vector
     * @param vel
     * @return
     */
    def move(vel: D3float, dt: Double): MovingImageVertex[T] =
      ArrayImageVertex.createAndMove(this,vel,dt)


  }


  /**
   * A point
   * @tparam T
   */
  trait MovingImageVertex[T] extends ImageVertex[T] {
    val velocity: D3float
    def get_history(): Array[D3float]
    def asIV: ImageVertex[T] = this
  }



  /**
   * A class for storing the image vertex information to prevent excessive tuple-dependence
   */
  case class SimpleImageVertex[T](index: Long, lat_pos: D3int = D3int(0,0,0), value: T,
                               original: Boolean = false, real_pos: Option[D3float] = None) extends
  ImageVertex[T] {
    override def shift(dx: Int, dy: Int, dz: Int): ImageVertex[T] = SimpleImageVertex[T](
      index,
      new D3int(lat_pos.x+dx,lat_pos.y+dy,lat_pos.z+dz),
      value,
      (dx==0 && dy==0 && dz==0)
    )
  }

  object ArrayImageVertex extends Serializable {
    def createAndMove[T](inIV: ImageVertex[T], newVel: D3float = D3float(0.0), dt: Double) =
    {
      // at the latest create the position here
      val prev_pos = inIV.real_pos.getOrElse(inIV.lat_pos.toF)

      inIV match {
        case aiv: MovingImageVertex[T] =>
          val new_vel = aiv.velocity + newVel
          ArrayImageVertex(aiv.index, aiv.lat_pos, aiv.value, aiv.original,
            new_vel,
            Some(prev_pos + (new_vel*dt)),
            aiv.get_history() ++ Array(prev_pos)
          )
        case iv: ImageVertex[T] =>
          ArrayImageVertex(iv.index, iv.lat_pos, iv.value, iv.original,
            newVel,
            Some(prev_pos + (newVel*dt)),
            Array(prev_pos)
            // at the latest create the position here
          )

      }

    }
  }
  case class ArrayImageVertex[T](index: Long, lat_pos: D3int = D3int(0,0,0), value: T = 0,
                                 original: Boolean = false, velocity: D3float = D3float(0.0),
                                 real_pos: Option[D3float] = None,
                                 last_pos: Array[D3float] = Array.empty[D3float])
    extends MovingImageVertex[T] {




    override def shift(dx: Int, dy: Int, dz: Int): ImageVertex[T] = {
      val orig = (dx==0 && dy==0 && dz==0)
      ArrayImageVertex(
        index,
        new D3int(lat_pos.x+dx,lat_pos.y+dy,lat_pos.z+dz),
        value,
        orig,
        velocity,
        real_pos,
        if (orig) last_pos else Array.empty[D3float]
      )
    }

    override def get_history(): Array[D3float] = last_pos
  }


  /**
   * A simple output class for use with SparkSQL and other tools
   * @param time time the point is representing
   * @param pos_x the vertex position in x
   * @param pos_y the vertex position in y
   * @param pos_z the vertex position in z
   */
  case class TimedPrimitiveVertex(time: Double, pos_x: Double, pos_y: Double, pos_z: Double,
                                   index: Long)

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
  implicit class ieSub[T](iv: ImageVertex[T]) extends Serializable {
    def -(iv2: ImageVertex[T]): ImageEdge = {
      val aPos = iv.real_pos.getOrElse(iv.lat_pos.toF)
      val bPos = iv2.real_pos.getOrElse(iv2.lat_pos.toF)
      val xd = aPos.x - bPos.x
      val yd = aPos.y - bPos.y
      val zd = aPos.z - bPos.z
      val bDist = Math.sqrt(Math.pow(xd, 2) + Math.pow(yd, 2) + Math.pow(zd, 2))
      new ImageEdge(bDist, types.D3float(xd / bDist, yd / bDist, zd / bDist), 1)
    }
  }
}