package com.azavea.ghive.jts.udf

import scala.util.Try

package object serializers {
  type TUnaryDeserializer[T]       = UnaryDeserializer[Try, T]
  type TBinaryDeserializer[T]      = BinaryDeserializer[Try, T, T]
  type TQuarternaryDeserializer[T] = QuarternaryDeserializer[Try, T, T, T, T]
}
