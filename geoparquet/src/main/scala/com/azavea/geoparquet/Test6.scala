package com.azavea.geoparquet

import com.github.davidmoten.rtree.{Entry, InternalStructure, OnSubscribeSearch, RTree, Serializer, Serializers}
import com.github.davidmoten.rtree.geometry.Geometry
import com.github.davidmoten.rtree.geometry.internal.RectangleDouble
import com.github.davidmoten.rtree.Serializers.Method
import org.apache.commons.lang3.Conversion
import rx.Observable
import rx.functions.Func1

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

object Test6 {
  implicit class IntOps(val value: Int) extends AnyVal {
    def toByteArray: Array[Byte] = {
      val result = new Array[Byte](4)
      Conversion.intToByteArray(value, 0, result, 0, 4)
    }
  }

  implicit class ArrayByteOps(val value: Array[Byte]) extends AnyVal {
    def toInt: Int = Conversion.byteArrayToInt(value, 0, 0, 0, 4)
  }

  implicit class SerializerBuilderOps(val self: Serializers.SerializerBuilder) extends AnyVal {
    type T = Int
    def make[S <: Geometry](serializer: Func1[T, Array[Byte]], deserializer: Func1[Array[Byte], T], method: Serializers.Method): Serializer[T, S] = {
      // classOf[Serializers.SerializerTypedBuilder].getDeclaredConstructors
      // get private cons
      val consl = classOf[Serializers.SerializerTypedBuilder[T]].getDeclaredConstructors
      // println(s"consl.length: ${consl.length}")
      val cons = consl.tail.head
      // make it public
      cons.setAccessible(true)
      cons
        .newInstance(serializer, deserializer, method)
        .asInstanceOf[Serializers.SerializerTypedBuilder[T]]
        .create[S]
    }

    def int: Serializer[Int, Geometry] = {
      val serializer = createIntSerializer
      val deserializer = createIntDeserializer
      make(serializer, deserializer, Method.FLATBUFFERS/*Method.KRYO*/) // Method.FLATBUFFERS
    }
  }

  private def createIntSerializer: Func1[Int, Array[Byte]] = new Func1[Int, Array[Byte]]() {
    override def call(i: Int): Array[Byte] = i.toByteArray
  }

  private def createIntDeserializer: Func1[Array[Byte], Int] = new Func1[Array[Byte], Int]() {
    override def call(bytes: Array[Byte]): Int = bytes.toInt
  }


  def mainStr(args: Array[String]): Unit = {
    val tree: RTree[String, Geometry] = RTree.minChildren(64).maxChildren(256).create()

    tree.add("1", RectangleDouble.create(0, 0, 20, 20))
    tree.add("2", RectangleDouble.create(0, 0, 30, 30))
    tree.add("3", RectangleDouble.create(0, 0, 30, 30))

    val os = new ByteArrayOutputStream()
    // val is = new ByteArrayInputStream()

    // Serializer[Int, Geometry]

    // val kk: Serializers.SerializerBuilder = Serializers.flatBuffers

    // val ser = Serializers.flatBuffers.utf8
    val ser: Serializer[String, Geometry] = Serializers.flatBuffers.utf8()

    ser.write(tree, os)
    os.close()

    val array = os.toByteArray
    val tree2 = ser.read(new ByteArrayInputStream(array), array.length, InternalStructure.SINGLE_ARRAY)

    println(tree)
    println(tree2)
  }

  def main(args: Array[String]): Unit = {
    // val treed = RTree.create
    // String the entry value
    // Geometry is the geometry value
    val tree: RTree[Int, Geometry] = RTree.minChildren(64).maxChildren(256).create()

    tree.add(1, RectangleDouble.create(0, 0, 20, 20))
    tree.add(2, RectangleDouble.create(0, 0, 30, 30))
    tree.add(3, RectangleDouble.create(0, 0, 30, 30))

    def test: Func1[Int, Array[Byte]] = new Func1[Int, Array[Byte]]() {
      override def call(i: Int): Array[Byte] = i.toByteArray
    }

    // def ssearch(condition: Func1[_ >: Geometry, Boolean]) =
      // tree.map((node: _$1) => Observable.unsafeCreate(new OnSubscribeSearch[T, S](node, condition))).orElseGet(Observable.empty)


    /**
     * Observable<Entry<T, S>> search(Func1<? super Geometry, Boolean> condition) {
        return root
                .map(node -> Observable.unsafeCreate(new OnSubscribeSearch<>(node, condition)))
                .orElseGet(Observable::empty);
    }
     */
    /*tree.entries().map {
      new Func1[Entry[Int, Geometry], Int]() {
        override def call(t: Entry[Int, Geometry]): Int = ???
      }
    }*/

    val os = new ByteArrayOutputStream()
    // val is = new ByteArrayInputStream()

    // Serializer[Int, Geometry]

    // val kk: Serializers.SerializerBuilder = Serializers.flatBuffers

    // val ser = Serializers.flatBuffers.utf8
    val ser: Serializer[Int, Geometry] = Serializers.flatBuffers.int

    ser.write(tree, os)
    os.close()

    val array = os.toByteArray
    val tree2 = ser.read(new ByteArrayInputStream(array), array.length, InternalStructure.SINGLE_ARRAY)

    println(tree)
    println(tree2)

    // idea
    // load all geoms via spark into mem
    // group by the spatial partitioner (good q what is the size of keys), should depend on the data
    // approximately split into even groups (somehow?)

    // insert geoms + group number into the rtree

    // how to build rtrees?
    // can we get the rtree idx number to build a tree and than traverse through all leaves and assign evet geom into
    // a row group

    // we can build the tree an list all entries https://github.com/plokhotnyuk/rtree2d/blob/master/rtree2d-core/shared/src/main/scala/com/github/plokhotnyuk/rtree2d/core/RTree.scala#L275-L278

  }
}
