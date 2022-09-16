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

import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for xpath expressions.
 */
class XPathFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("xpath_boolean") {
    val df = Seq("<a><b>b</b></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_boolean(xml, 'a/b')"), Row(true))

    // non-foldable path argument
    val df2 = Seq(
      ("<a><b>b</b></a>", "a/b"),
      ("<a><b>b</b></a>", "a/c")).toDF("xml", "path")
    checkAnswer(df2.selectExpr("xpath_boolean(xml, path)"),
      Row(true) :: Row(false) :: Nil)
  }

  test("xpath_short, xpath_int, xpath_long") {
    val df = Seq("<a><b>1</b><b>2</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_short(xml, 'sum(a/b)')",
        "xpath_int(xml, 'sum(a/b)')",
        "xpath_long(xml, 'sum(a/b)')"),
      Row(3.toShort, 3, 3L))

    // non-foldable path argument
    val df2 = Seq(
      ("<a><b>1</b><b>2</b></a>", "sum(a/b)"),
      ("<a><c>1</c><c>3</c></a>", "sum(a/c)")).toDF("xml", "path")
    checkAnswer(
      df2.selectExpr(
        "xpath_short(xml, path)",
        "xpath_int(xml, path)",
        "xpath_long(xml, path)"),
      Row(3.toShort, 3, 3L) :: Row(4.toShort, 4, 4L) :: Nil)
  }

  test("xpath_float, xpath_double, xpath_number") {
    val df = Seq("<a><b>1.0</b><b>2.1</b></a>").toDF("xml")
    checkAnswer(
      df.selectExpr(
        "xpath_float(xml, 'sum(a/b)')",
        "xpath_double(xml, 'sum(a/b)')",
        "xpath_number(xml, 'sum(a/b)')"),
      Row(3.1.toFloat, 3.1, 3.1))

    // non-foldable path argument
    val df2 = Seq(
      ("<a><b>1.0</b><b>2.1</b></a>", "sum(a/b)"),
      ("<a><c>1.0</c><c>3.1</c></a>", "sum(a/c)")).toDF("xml", "path")
    checkAnswer(
      df2.selectExpr(
        "xpath_float(xml, path)",
        "xpath_double(xml, path)",
        "xpath_number(xml, path)"),
    Row(3.1.toFloat, 3.1, 3.1) :: Row(4.1.toFloat, 4.1, 4.1) :: Nil)
  }

  test("xpath_string") {
    val df = Seq("<a><b>b</b><c>cc</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath_string(xml, 'a/c')"), Row("cc"))

    // non-foldable path argument
    val df2 = Seq(
      ("<a><b>b</b><c>cc</c></a>", "a/c"),
      ("<a><b>x</b><c>cc</c></a>", "a/b")).toDF("xml", "path")
    checkAnswer(
      df2.selectExpr("xpath_string(xml, path)"),
      Row("cc") :: Row("x") :: Nil)
  }

  test("xpath") {
    val df = Seq("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>").toDF("xml")
    checkAnswer(df.selectExpr("xpath(xml, 'a/*/text()')"), Row(Seq("b1", "b2", "b3", "c1", "c2")))

    // non-foldable path argument
    val df2 = Seq(
      ("<a><b>b</b><c>cc</c></a>", "a/c/text()"),
      ("<a><b>x</b><c>cc</c></a>", "a/b/text()")).toDF("xml", "path")
    checkAnswer(
      df2.selectExpr("xpath(xml, path)"),
      Row(Seq("cc")) :: Row(Seq("x")) :: Nil)
  }
}
