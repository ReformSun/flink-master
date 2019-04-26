package org.apache.flink.table.api.java

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.runtime.types.CRow

object WindowOutputTag {
  private var input:DataStream[CRow] = null
  def setDataStream(input:DataStream[CRow]): Unit ={
    this.input = input
  }

  def getDataStream:DataStream[CRow] = input
}
