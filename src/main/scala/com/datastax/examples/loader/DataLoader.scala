package com.datastax.examples.loader

import org.apache.commons.csv.{CSVFormat, CSVRecord}
import org.janusgraph.core.{JanusGraph, JanusGraphTransaction, JanusGraphVertex}

import java.io.FileReader
import java.text.SimpleDateFormat
import java.util
import java.util.Date

object DataLoader {

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
      val transaction = g.newTransaction()

      while (iter.hasNext) {
        val propMap = iter.next().toMap
        val v: JanusGraphVertex = classOf[JanusGraphTransaction]
          .getMethod("addVertex", classOf[String])
          .invoke(transaction, vertexLabel)
          .asInstanceOf[JanusGraphVertex]

        propMap
          .keySet()
          .forEach(propName =>
            v.property(
              propName,
              datatypeConverter(propMap.get(propName), transaction.getPropertyKey(propName).dataType())
            )
          )
      }

      transaction.commit()
    }

    // E loading
    edgeLabelFilePathMap.keys.foreach { edgeLabel =>
      val edgeFilePath = edgeLabelFilePathMap(edgeLabel)("data")
      val leftVLabel = edgeLabelFilePathMap(edgeLabel)("left")
      val rightVLabel = edgeLabelFilePathMap(edgeLabel)("right")

      val in = new FileReader(edgeFilePath)
      val iter: util.Iterator[CSVRecord] = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in).iterator
      val transaction = g.newTransaction()

      while (iter.hasNext) {
        val propMap = iter.next().toMap

        val leftNode = transaction.traversal().V().hasLabel(leftVLabel).has("node_id", propMap.get("Left")).next()
        val rightNode = transaction.traversal().V().hasLabel(rightVLabel).has("node_id", propMap.get("Right")).next()

        val edge = leftNode.addEdge(edgeLabel, rightNode)

        propMap.keySet().forEach { propName =>
          if (!propName.equalsIgnoreCase("left") && !propName.equalsIgnoreCase("right"))
            edge.property(
              propName,
              datatypeConverter(propMap.get(propName), transaction.getPropertyKey(propName).dataType())
            )
        }
      }

      transaction.commit()
    }
  }

  def datatypeConverter(value: String, dataType: Class[_]): Object =
    if (dataType == classOf[Date]) {
      val dateParser = new SimpleDateFormat("dd-MMM-yyyy")
      dateParser.parse(value)
    } else
      value
}
