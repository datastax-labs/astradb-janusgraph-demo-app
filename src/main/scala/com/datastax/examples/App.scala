package com.datastax.examples

import com.datastax.examples.loader.DataLoader
import com.datastax.examples.schema.SchemaLoader
import org.janusgraph.core.JanusGraphFactory
import org.json4s.jackson.JsonMethods.parse

object App {
  def main(args: Array[String]): Unit = {
    implicit val formats = org.json4s.DefaultFormats

    if (args.length != 1) {
      println("Usage: App <executorConf.json>")
      System.exit(-1)
    }

    val conf = parse(scala.io.Source.fromFile(args(0)).mkString).extract[Map[String, Any]]

    val g = { // replacement for drop graph if exists
      val oldg = JanusGraphFactory.open(conf("janusGraphPropertiesFile").toString)
      JanusGraphFactory.drop(oldg)
      JanusGraphFactory.open(conf("janusGraphPropertiesFile").toString)
    }

    try {
      SchemaLoader
        .loadSchema(g, conf("schemaPath").toString)
      DataLoader.loadData(
        g,
        conf("vertexLabelFilePathMap").asInstanceOf[Map[String, String]],
        conf("edgeLabelFilePathMap").asInstanceOf[Map[String, Map[String, String]]]
      )
    } catch {
      case e: Exception =>
        println(s"Exception occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally g.close()
  }
}
