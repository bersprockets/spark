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

package org.apache.spark.sql.execution.benchmark

/**
 * Benchmark to measure performance for joins.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/JoinBenchmark-results.txt".
 * }}}
 */
object SetOpsBenchmark extends SqlBasedBenchmark {
  def exceptAllWithManyDuplicates(): Unit = {
    val ct1 = 20000000    // the number of rows in first table
    val dupCt = 4000000   // the number of dup rows per key
    val keyCt = ct1/dupCt // the number of unique keys
    val ct2 = 1000        // the number of rows in second table
    val tbl1 = spark.range(ct1).selectExpr(s"id % $keyCt as k")
    val tbl2 = spark.range(ct2).selectExpr(s"id % $keyCt as k")
    codegenBenchmark("Except All With Many Duplicates", ct1) {
      val df = tbl1.exceptAll(tbl2)
      df.noop()
    }
  }

  def exceptAllWithSomeDuplicates(): Unit = {
    val ct1 = 20000000    // the number of rows in first table
    val dupCt = 1000      // the number of dup rows per key
    val keyCt = ct1/dupCt // the number of unique keys
    val ct2 = 1000        // the number of rows in second table
    val tbl1 = spark.range(ct1).selectExpr(s"id % $keyCt as k")
    val tbl2 = spark.range(ct2).selectExpr(s"id % $keyCt as k")
    codegenBenchmark("Except All With Some Duplicates", ct1) {
      val df = tbl1.exceptAll(tbl2)
      df.noop()
    }
  }

  def exceptAllWithNoDuplicates(): Unit = {
    val ct1 = 20000000    // the number of rows in first table
    val ct2 = 1000        // the number of rows in second table
    val tbl1 = spark.range(ct1).selectExpr("id as k")
    val tbl2 = spark.range(ct2).selectExpr("id as k")
    codegenBenchmark("Except All With No Duplicates", ct1) {
      val df = tbl1.exceptAll(tbl2)
      df.noop()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    exceptAllWithManyDuplicates()
    exceptAllWithSomeDuplicates()
    exceptAllWithNoDuplicates()
  }
}
