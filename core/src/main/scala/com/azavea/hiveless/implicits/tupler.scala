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

package com.azavea.hiveless.implicits

import shapeless.{Generic, HList, IsTuple}
import shapeless.ops.function.FnToProduct

object tupler extends Serializable {
  // format: off
  /**
   * Tuples FunctionN.
   * Converts FunctionN(arg1, ..., argn) => {} into arg1 :: ... :: argn :: HList => {} via FnToProduct
   * T is a generic tuple, converted into HList via Generic and applied to the HList function
   */
  // format: on
  implicit def tuplerGeneric[F, I <: HList, O, T: IsTuple](f: F)(implicit ftp: FnToProduct.Aux[F, I => O], gen: Generic.Aux[T, I]): T => O = { t: T =>
    ftp(f)(gen.to(t))
  }
}
