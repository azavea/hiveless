package com.azavea.geoparquet

import com.github.plokhotnyuk.rtree2d.core.RTree

import scala.annotation.tailrec

object Test5 {
  import com.github.plokhotnyuk.rtree2d.core._
  import EuclideanPlane._

  def main(args: Array[String]): Unit = {
    val box1 = entry(1.0f, 1.0f, 2.0f, 2.0f, "Box 1")
    val box2 = entry(2.0f, 2.0f, 3.0f, 3.0f, "Box 2")
    val entries = Seq(box1, box2)

    val rtree = RTree(entries)



  }
}
