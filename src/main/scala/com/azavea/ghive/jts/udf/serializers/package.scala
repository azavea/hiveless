/*
 * Copyright 2021 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.azavea.ghive.jts.udf

import scala.util.Try

package object serializers {
  type TUnaryDeserializer[T]                    = UnaryDeserializer[Try, T]
  type TBinaryDeserializer[T0, T1]              = BinaryDeserializer[Try, T0, T1]
  type TTernaryDeserializer[T0, T1, T2]         = TernaryDeserializer[Try, T0, T1, T2]
  type TQuarternaryDeserializer[T0, T1, T2, T3] = QuarternaryDeserializer[Try, T0, T1, T2, T3]
}
