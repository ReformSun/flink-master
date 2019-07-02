package org.apache.flink.table.runtime.aggregate

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.CRow

class CustomAllTimeWindowMap(
                              private val windowStartOffset: Option[Int],
                              private val windowEndOffset: Option[Int],
                              private val windowRowtimeOffset: Option[Int],
                              private val finalRowArity: Int,
                              private val rowTimeIndex: Int) extends RichMapFunction[Row,CRow]{
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def map(value: Row): CRow = {
    var output: Row = new Row(finalRowArity);
    val lastFieldPos = output.getArity - 1
    var num : Int = 0;

    if (windowStartOffset.isDefined) {
      num = num + 1
      output.setField(
        lastFieldPos + windowStartOffset.get,
        SqlFunctions.internalToTimestamp(value.getField(rowTimeIndex).asInstanceOf[Long]))
    }
    if (windowEndOffset.isDefined) {
      num = num + 1
      output.setField(
        lastFieldPos + windowEndOffset.get,
        SqlFunctions.internalToTimestamp(value.getField(rowTimeIndex).asInstanceOf[Long]))
    }

    var i = 0
    var j = 0
    while (i < value.getArity) {
      if (i != rowTimeIndex){
        output.setField(j, value.getField(i))
        j += 1
      }
      i += 1
    }
    new CRow(output,true)
  }

}
