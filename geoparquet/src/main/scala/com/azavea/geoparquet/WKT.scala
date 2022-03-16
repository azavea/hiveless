package com.azavea.geoparquet

import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.locationtech.jts.geom._

object WKT {
  private val readerBox = new ThreadLocal[WKTReader]
  private val writerBox = new ThreadLocal[WKTWriter]

  def read(value: String): Geometry = {
    if (readerBox.get == null) readerBox.set(new WKTReader())
    readerBox.get.read(value)
  }

  def write(geom: Geometry): String = {
    if (writerBox.get == null) writerBox.set(new WKTWriter())
    writerBox.get.write(geom)
  }
}
