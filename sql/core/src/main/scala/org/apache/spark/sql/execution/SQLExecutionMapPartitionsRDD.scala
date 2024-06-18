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

package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.SparkSession

class SQLExecutionMapPartitionsRDD[U: ClassTag, T: ClassTag](
    @transient session: SparkSession,
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning, isFromBarrier, isOrderSensitive) {

  override def map[V: ClassTag](f: U => V): RDD[V] = withScope {
    val cleanedF = session.sparkContext.clean(f)
    new SQLExecutionMapPartitionsRDD[V, U](session, this, (_, _, iter) => iter.map(cleanedF))
  }

  override def mapPartitions[V: ClassTag](
      f: Iterator[U] => Iterator[V],
      preservesPartitioning: Boolean = false): RDD[V] = {
    val cleanedF = session.sparkContext.clean(f)
    new SQLExecutionMapPartitionsRDD(
      session,
      this,
      (_: TaskContext, _: Int, iter: Iterator[U]) => cleanedF(iter),
      preservesPartitioning).asInstanceOf[RDD[V]]
  }

  override def collect(): Array[U] = {
    SQLExecution.withSQLConfPropagated(session) {
      super.collect()
    }
  }
}
