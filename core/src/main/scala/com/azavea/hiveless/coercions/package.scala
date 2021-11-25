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

package com.azavea.hiveless

import shapeless.{::, HNil}

package object coercions {
  implicit def function1Tupler[T, R](f: T => R): (T :: HNil) => R                                                = { case head :: HNil => f(head) }
  implicit def function2Tupler[T0, T1, R](f: (T0, T1) => R): (T0 :: T1 :: HNil) => R                             = { hlist => f.tupled(hlist.tupled) }
  implicit def function3Tupler[T0, T1, T2, R](f: (T0, T1, T2) => R): (T0 :: T1 :: T2 :: HNil) => R               = { hlist => f.tupled(hlist.tupled) }
  implicit def function4Tupler[T0, T1, T2, T3, R](f: (T0, T1, T2, T3) => R): (T0 :: T1 :: T2 :: T3 :: HNil) => R = { hlist => f.tupled(hlist.tupled) }
}
