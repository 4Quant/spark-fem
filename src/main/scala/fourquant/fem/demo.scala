package fourquant.fem

import org.apache.spark.SparkContext

/**
 * Run a demo from the command line
 */
  object demo extends Serializable {
    def main(args: Array[String]): Unit = {
      val nsc = new SparkContext("local[4]","FEM-Demo")

    }
  }
