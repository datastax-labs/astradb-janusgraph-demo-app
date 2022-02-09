package com.datastax.examples.loader

import org.apache.commons.csv.{CSVFormat, CSVRecord}
import org.janusgraph.core.JanusGraph

import java.io.FileReader
import java.util

class DataLoader {

  /**
    * @param g
    * @param vertexLabelFilePathMap -> <vertexLabel -> vertexFilePath>
    * @param edgeLabelFilePathMap   -> <edgeLabel -> <data -> <dataPath>, <left> -> <leftVertexLabel>, <right> -> <rightVertexLabel>>
    */
  def loadData(
    g: JanusGraph,
    vertexLabelFilePathMap: Map[String, String],
    edgeLabelFilePathMap: Map[String, Map[String, String]]
  ) = {
    // V loading
    vertexLabelFilePathMap.keys.foreach { vertexLabel =>
      val vertexFilePath = vertexLabelFilePathMap(vertexLabel)
      val in = new FileReader(vertexFilePath)
      val iter: util.Iterator[CSVRecord] = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in).iterator
      val traversal = g.traversal()

      while (iter.hasNext) {
        val propMap = iter.next().toMap
        val v = traversal.addV(vertexLabel)

        propMap.keySet().forEach(propName => v.property(propName, propMap.get(propName)))
      }

      traversal.close()
    }

    // E loading
    edgeLabelFilePathMap.keys.foreach { edgeLabel =>
      val edgeFilePath = edgeLabelFilePathMap(edgeLabel)("data")
      val leftVLabel = edgeLabelFilePathMap(edgeLabel)("left")
      val rightVLabel = edgeLabelFilePathMap(edgeLabel)("right")

      val in = new FileReader(edgeFilePath)
      val iter: util.Iterator[CSVRecord] = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in).iterator
      val traversal = g.traversal()

      while (iter.hasNext) {
        val propMap = iter.next().toMap

        val leftNode = traversal.V().hasLabel(leftVLabel).has("node_id", propMap.get("Left")).next()
        val rightNode = traversal.V().hasLabel(rightVLabel).has("node_id", propMap.get("Right")).next()

        val edge = leftNode.addEdge(edgeLabel, rightNode)

        propMap.keySet().forEach { propName =>
          if (!propName.equalsIgnoreCase("left") && !propName.equalsIgnoreCase("right"))
            edge.property(propName, propMap.get(propName))
        }
      }

      traversal.close()
    }
  }
}
