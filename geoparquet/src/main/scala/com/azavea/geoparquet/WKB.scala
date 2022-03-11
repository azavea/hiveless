package com.azavea.geoparquet

import org.locationtech.jts.geom._
import org.locationtech.jts.{io => jts}

object WKB {
  private val readerBox = new ThreadLocal[jts.WKBReader]
  private val writerBox = new ThreadLocal[jts.WKBWriter]

  /** Convert Well Known Binary to Geometry */
  def read(value: Array[Byte]): Geometry = {
    if (readerBox.get == null) readerBox.set(new jts.WKBReader())
    readerBox.get.read(value)
  }

  /** Convert Well Known Binary to Geometry */
  def read(hex: String): Geometry = {
    if (readerBox.get == null) readerBox.set(new jts.WKBReader())
    readerBox.get.read(jts.WKBReader.hexToBytes(hex))
  }

  /** Convert Geometry to Well Known Binary */
  def write(geom: Geometry): Array[Byte] = {
    if (writerBox.get == null) writerBox.set(new jts.WKBWriter(2))
    writerBox.get.write(geom)
  }
}