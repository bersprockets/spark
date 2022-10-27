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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.types.ArrayType

/**
 * Updates nullability of Attributes in a resolved LogicalPlan by using the nullability of
 * corresponding Attributes of its children output Attributes. This step is needed because
 * users can use a resolved AttributeReference in the Dataset API and outer joins
 * can change the nullability of an AttributeReference. Without this rule, a nullable column's
 * nullable field can be actually set as non-nullable, which cause illegal optimization
 * (e.g., NULL propagation) and wrong answers.
 * See SPARK-13484 and SPARK-13801 for the concrete queries of this case.
 */
object UpdateAttributeNullability extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    AlwaysProcess.fn, ruleId) {
    // Skip unresolved nodes.
    case p if !p.resolved => p
    // Skip leaf node, as it has no child and no need to update nullability.
    case p: LeafNode => p
    case p: LogicalPlan if p.childrenResolved =>
      val nullabilities = p.children.flatMap(c => c.output).groupBy(_.exprId).map {
        // If there are multiple Attributes having the same ExprId, we need to resolve
        // the conflict of nullable field. We do not really expect this to happen.
        case (exprId, attributes) => exprId -> attributes.exists(_.nullable)
      }
      // For an Attribute used by the current LogicalPlan, if it is from its children,
      // we fix the nullable field by using the nullability setting of the corresponding
      // output Attribute from the children.
      val p2 = p.transformExpressions {
        case attr: Attribute if nullabilities.contains(attr.exprId) =>
          attr.withNullability(nullabilities(attr.exprId))
      }

      val arrayNullabilities = p.children.flatMap(c => c.output)
        .filter(c => c.dataType.isInstanceOf[ArrayType]).groupBy(_.exprId).map {
        case (exprId, attributes) =>
          exprId -> attributes.exists(_.dataType.asInstanceOf[ArrayType].containsNull)
      }

      val newP = p2.transformExpressions {
        case attr: Attribute if arrayNullabilities.contains(attr.exprId) =>
          val dt = attr.dataType.asInstanceOf[ArrayType]
          val newDt = dt.copy(containsNull = arrayNullabilities(attr.exprId))
          if (dt.containsNull != newDt.containsNull) {
            // print(s"Updating containsNull for ${attr} in operator ${p2.getClass.getName}\n")
          }
          attr.withDataType(newDt)
      }
      newP match {
        case gen: Generate =>
          val generator = gen.generator
          val arrayChildren = generator.children.filter(c => c.dataType.isInstanceOf[ArrayType])
          val arrayChildrenCN = arrayChildren.map(_.dataType.asInstanceOf[ArrayType].containsNull)
          // print(s"Generator ${generator} child containsNull is ${arrayChildrenCN}, output is\n")
          gen.generatorOutput.foreach { a =>
            // print(s"  ${a}, nullable ${a.nullable}\n")
          }
        case _ =>
      }
      newP match {
        case gen: Generate =>
          val schemaOutput = gen.generator.elementSchema.zip(gen.generatorOutput)
          val newGenOutput = schemaOutput.map { case (sf, a) =>
            a.withNullability(sf.nullable)
          }
          val newGen = gen.copy(generatorOutput = newGenOutput)
          // print(s"newP output: ${newP.output}, newGen output ${newGen.output}\n")
          newGen
        case lp @_ =>
          lp
      }
  }
}
