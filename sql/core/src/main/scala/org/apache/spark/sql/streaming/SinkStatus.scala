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

package org.apache.spark.sql.streaming

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.streaming.StreamingQueryStatus.indent

/**
 * :: Experimental ::
 * Status and metrics of a streaming sink.
 *
 * @param description Description of the source corresponding to this status.
 * @param offsetDesc Description of the current offsets up to which data has been written
 *                   by the sink.
 * @since 2.0.0
 */
@Experimental
class SinkStatus private(
    val description: String,
    val offsetDesc: String) {

  /** The compact JSON representation of this status. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String =
    "Status of sink " + indent(prettyString).trim

  private[sql] def jsonValue: JValue = {
    ("description" -> JString(description)) ~
    ("offsetDesc" -> JString(offsetDesc))
  }

  private[sql] def prettyString: String = {
    s"""$description
       |Committed offsets: $offsetDesc
       |""".stripMargin
  }
}

/** Companion object, primarily for creating SinkStatus instances internally */
private[sql] object SinkStatus {
  def apply(desc: String, offsetDesc: String): SinkStatus = new SinkStatus(desc, offsetDesc)
}
