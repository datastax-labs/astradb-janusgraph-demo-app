package com.datastax.examples.schema

import java.util.Date

object TypeMaps {
  val datatypeMap = Map(
    "Integer" -> classOf[Integer],
    "String" -> classOf[String],
    "Date" -> classOf[Date]
  )
}

case class PropertyKey(name: String, dataType: String, cardinality: String)

case class VertexLabel(name: String)

case class EdgeLabel(name: String, multiplicity: String)

case class VertexIndex(name: String, propertyKeys: List[String], isComposite: Boolean, isUnique: Boolean)

case class GraphSchema(
  propertyKeys: List[PropertyKey],
  vertexLabels: List[VertexLabel],
  edgeLabels: List[EdgeLabel],
  vertexIndexes: List[VertexIndex]
) {
  def this() = this(null, null, null, null)
}
