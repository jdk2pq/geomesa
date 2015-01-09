/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa

import org.calrissian.mango.types.LexiTypeEncoders

package object raster {
  def lexiEncodeDoubleToString(number: Double): String = {
    val truncatedRes = BigDecimal(number).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble
    LexiTypeEncoders.LEXI_TYPES.encode(truncatedRes)
  }

  def lexiDecodeStringToDouble(str: String): Double = {
    val number = LexiTypeEncoders.LEXI_TYPES.decode("double", str).asInstanceOf[Double]
    BigDecimal(number).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
