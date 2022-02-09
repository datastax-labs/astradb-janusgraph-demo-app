package com.datastax.examples.schema

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.{Cardinality, JanusGraph, Multiplicity}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object SchemaLoader {
  def getGraphSchema(graphSchemaStr: String): GraphSchema = {
    implicit val formats = org.json4s.DefaultFormats

    val m = parse(graphSchemaStr).extract[Map[String, Any]]

    // extract property keys
    val pKeys = m("propertyKeys").asInstanceOf[List[Map[String, String]]].map { p =>
      PropertyKey(p("name"), p("dataType"), p("cardinality"))
    }

    // extract vertexLabels
    val vertexLabels = m("vertexLabels").asInstanceOf[List[Map[String, String]]].map(vl => VertexLabel(vl("name")))

    // extract edgeLabels
    val edgeLabels =
      m("edgeLabels").asInstanceOf[List[Map[String, String]]].map(el => EdgeLabel(el("name"), el("multiplicity")))

    // extract vertexIndexes
    val vertexIndexes = m("vertexIndexes")
      .asInstanceOf[List[Map[String, Any]]]
      .map { vi =>
        VertexIndex(
          vi("name").toString,
          vi("propertyKeys").asInstanceOf[List[String]],
          vi("composite").toString.toBoolean,
          vi("unique").toString.toBoolean
        )
      }

    // collated graphSchema
    GraphSchema(pKeys, vertexLabels, edgeLabels, vertexIndexes)
  }

  def loadSchema(g: JanusGraph, schemaFilePath: String): Unit = {
    val graphSchema = getGraphSchema(scala.io.Source.fromFile(schemaFilePath).mkString)
    val mgmt = g.openManagement()

    try {
      // setup property keys
      graphSchema.propertyKeys.foreach { p =>
        mgmt
          .makePropertyKey(p.name)
          .dataType(TypeMaps.datatypeMap(p.dataType))
          .cardinality(Cardinality.valueOf(p.cardinality))
          .make()
        println(s"propertyKey:${p.name} is created")
      }

      // setup vertexLabels
      graphSchema.vertexLabels.foreach { vl =>
        val vlMaker = mgmt.makeVertexLabel(vl.name)
        vlMaker.make()
        println(s"vertexLabel:${vl.name} is created")
      }

      // setup edgeLabels
      graphSchema.edgeLabels.foreach { el =>
        val elMaker = mgmt.makeEdgeLabel(el.name).multiplicity(Multiplicity.valueOf(el.multiplicity))
        elMaker.make()
        println(s"edgeLabel:${el.name} is created")
      }
      mgmt.commit()

      val mgmtForIndexes = g.openManagement()
      try {
        // setup vertexIndexes
        graphSchema.vertexIndexes.foreach { vi =>
          if (!mgmtForIndexes.containsGraphIndex(vi.name)) {
            val indexBuilder = mgmtForIndexes.buildIndex(vi.name, classOf[Vertex])
            vi.propertyKeys.foreach(p => indexBuilder.addKey(mgmtForIndexes.getPropertyKey(p)))

            if (vi.isUnique)
              indexBuilder.unique()

            if (vi.isComposite)
              indexBuilder.buildCompositeIndex()

            println(s"vertexIndex:${vi.name} is created")
          } else
            println(s"vertexIndex:${vi.name} already exists")
        }
        mgmtForIndexes.commit()
      } catch {
        case e: Exception =>
          println(s"Exception occurred during Schema Index setup ${e.getMessage} ... Rolling back")
          mgmtForIndexes.rollback()
          mgmt.rollback()
      }
    } catch {
      case e: Exception =>
        println(s"Exception occurred during Schema setup ${e.getMessage} ... Rolling back")
        e.printStackTrace()
        mgmt.rollback()
    }
  }
}
