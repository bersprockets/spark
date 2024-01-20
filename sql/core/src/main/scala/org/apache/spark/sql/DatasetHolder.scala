/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.annotation.Stable
import org.apache.spark.util.Utils

/**
 * A container for a [[Dataset]], used for implicit conversions in Scala.
 *
 * To use this, import implicit conversions in SQL:
 * {{{
 *   val spark: SparkSession = ...
 *   import spark.implicits._
 * }}}
 *
 * @since 1.6.0
 */
@Stable
case class DatasetHolder[T] private[sql](private val ds: Dataset[T]) {

  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDS("1")` as invoking this toDS and then apply on the returned Dataset.
  def toDS(): Dataset[T] = {
    if (Utils.isTesting) {
      ds.persist()
    } else {
      ds
    }
  }

  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = {
    val df = ds.toDF()
    if (Utils.isTesting) {
      df.persist()
    } else {
      df
    }
  }

  def toDF(colNames: String*): DataFrame = {
    val df = ds.toDF(colNames : _*)
    if (Utils.isTesting) {
      df.persist()
    } else {
      df
    }
  }
}
