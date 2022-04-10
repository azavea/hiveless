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

package com.azavea.hiveless.utils

import shapeless.{:+:, ::, CNil, Coproduct, Generic, HList, HNil, IsTuple}

import scala.reflect.{classTag, ClassTag}

/** Like a regular show, but accepts no arguments. */
trait HShow[T] extends Serializable {
  def show(): String
}

object HShow extends LowPriorityHShow {
  def apply[T](implicit ev: HShow[T]): HShow[T] = ev

  /** Derive HShow for Tuples. */
  implicit def hshowGeneric[T: IsTuple, L <: HList](implicit gen: Generic.Aux[T, L], sl: HShow[L]): HShow[T] =
    () => sl.show()

  /** Derive HShow for HList. */
  implicit val hshowHNil: HShow[HNil]                                                                  = () => ""
  implicit def hshowHCons[H: ClassTag, T <: HList](implicit sh: HShow[H], st: HShow[T]): HShow[H :: T] = () => s"${sh.show()}, ${st.show()}"

  /** Derive HShow for Coproduct. */
  implicit val hshowCNil: HShow[CNil]                                                                       = () => ""
  implicit def hshowCCons[H: ClassTag, T <: Coproduct](implicit sh: HShow[H], st: HShow[T]): HShow[H :+: T] = () => s"${sh.show()}, ${st.show()}"
}

trait LowPriorityHShow extends Serializable {

  /** HShow low priority ClassTag instance. */
  implicit def hshowClassTag[T: ClassTag]: HShow[T] = () => classTag[T].toString
}
