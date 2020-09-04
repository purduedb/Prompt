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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._

/**
 * Utility functions for defining window in DataFrames.
 *
 * {{{
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   Window.partitionBy("country").orderBy("date")
 *     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
 *
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 * }}}
 *
 * @since 1.4.0
 */
@InterfaceStability.Stable
object Window {

  /**
   * Creates a [[WindowSpec]] with the partitioning defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    spec.partitionBy(colName, colNames : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the partitioning defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    spec.partitionBy(cols : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the ordering defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    spec.orderBy(colName, colNames : _*)
  }

  /**
   * Creates a [[WindowSpec]] with the ordering defined.
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    spec.orderBy(cols : _*)
  }

  /**
   * Value representing the last row in the partition, equivalent to "UNBOUNDED PRECEDING" in SQL.
   * This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   * }}}
   *
   * @since 2.1.0
   */
  def unboundedPreceding: Long = Long.MinValue

  /**
   * Value representing the last row in the partition, equivalent to "UNBOUNDED FOLLOWING" in SQL.
   * This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
   * }}}
   *
   * @since 2.1.0
   */
  def unboundedFollowing: Long = Long.MaxValue

  /**
   * Value representing the current row. This can be used to specify the frame boundaries:
   *
   * {{{
   *   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   * }}}
   *
   * @since 2.1.0
   */
  def currentRow: Long = 0

  /**
   * Creates a [[WindowSpec]] with the frame boundaries defined,
   * from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   *
   * We recommend users use [[Window.unboundedPreceding]], [[Window.unboundedFollowing]],
   * and [[Window.currentRow]] to specify special boundary values, rather than using integral
   * values directly.
   *
   * @param start boundary start, inclusive. The frame is unbounded if this is
   *              the minimum long value ([[Window.unboundedPreceding]]).
   * @param end boundary end, inclusive. The frame is unbounded if this is the
   *            maximum long value  ([[Window.unboundedFollowing]]).
   * @since 2.1.0
   */
  // Note: when updating the doc for this method, also update WindowSpec.rowsBetween.
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    spec.rowsBetween(start, end)
  }

  /**
   * Creates a [[WindowSpec]] with the frame boundaries defined,
   * from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative from the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   *
   * We recommend users use [[Window.unboundedPreceding]], [[Window.unboundedFollowing]],
   * and [[Window.currentRow]] to specify special boundary values, rather than using integral
   * values directly.
   *
   * @param start boundary start, inclusive. The frame is unbounded if this is
   *              the minimum long value ([[Window.unboundedPreceding]]).
   * @param end boundary end, inclusive. The frame is unbounded if this is the
   *            maximum long value  ([[Window.unboundedFollowing]]).
   * @since 2.1.0
   */
  // Note: when updating the doc for this method, also update WindowSpec.rangeBetween.
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    spec.rangeBetween(start, end)
  }

  private[sql] def spec: WindowSpec = {
    new WindowSpec(Seq.empty, Seq.empty, UnspecifiedFrame)
  }

}

/**
 * Utility functions for defining window in DataFrames.
 *
 * {{{
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   Window.partitionBy("country").orderBy("date").rowsBetween(Long.MinValue, 0)
 *
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 * }}}
 *
 * @since 1.4.0
 */
@InterfaceStability.Stable
class Window private()  // So we can see Window in JavaDoc.
