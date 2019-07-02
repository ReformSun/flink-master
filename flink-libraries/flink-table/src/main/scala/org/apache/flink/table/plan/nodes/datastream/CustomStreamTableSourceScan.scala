package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, TimestampAssigner}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sources.{RowtimeAttributeDescriptor, StreamTableSource, TableSource, TableSourceUtil}
import org.apache.flink.table.sources.wmstrategies.{PeriodicWatermarkAssigner, PreserveWatermarks, PunctuatedWatermarkAssigner, WatermarkStrategy}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

class CustomStreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: StreamTableSource[_],
    selectedFields: Option[Array[Int]])
  extends PhysicalTableSourceScan(cluster, traitSet, table, tableSource, selectedFields)
  with StreamScan {

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    TableSourceUtil.getRelDataType(
      tableSource,
      selectedFields,
      streaming = true,
      flinkTypeFactory)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new CustomStreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource,
      selectedFields
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      newTableSource: TableSource[_]): PhysicalTableSourceScan = {

    new CustomStreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      newTableSource.asInstanceOf[StreamTableSource[_]],
      selectedFields
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = true,
      selectedFields)

    val config = tableEnv.getConfig
    val inputDataStream = tableSource.getDataStream(tableEnv.execEnv).asInstanceOf[DataStream[Any]]
    val outputSchema = new RowSchema(this.getRowType)

    // check that declared and actual type of table source DataStream are identical
    if (inputDataStream.getType != tableSource.getReturnType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of type ${inputDataStream.getType} that does not match with the " +
        s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      selectedFields,
      cluster,
      tableEnv.getRelBuilder,
      TimeIndicatorTypeInfo.ROWTIME_INDICATOR
    )

    // ingest table and convert and extract time attributes if necessary
    val ingestedTable = convertToInternalRow(
      outputSchema,
      inputDataStream,
      fieldIndexes,
      config,
      rowtimeExpression)

    // generate watermarks for rowtime indicator
    val rowtimeDesc: Option[RowtimeAttributeDescriptor] =
      TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, selectedFields)

    val withWatermarks = if (rowtimeDesc.isDefined) {
      val rowtimeFieldIdx = outputSchema.fieldNames.indexOf(rowtimeDesc.get.getAttributeName)
      val watermarkStrategy = rowtimeDesc.get.getWatermarkStrategy
      // 设置水印执行策略
      CustomStreamTableSourceScan.setWatermarkStrategy(watermarkStrategy)
      watermarkStrategy match {
        case p: PeriodicWatermarkAssigner =>
          val watermarkGenerator = new CustomPeriodicWatermarkAssignerWrapper(rowtimeFieldIdx, p)
          ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
        case p: PunctuatedWatermarkAssigner =>
          val watermarkGenerator = new CustomPunctuatedWatermarkAssignerWrapper(rowtimeFieldIdx, p)
          ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
        case _: PreserveWatermarks =>
          // The watermarks have already been provided by the underlying DataStream.
          ingestedTable
      }
    } else {
      // No need to generate watermarks if no rowtime attribute is specified.
      ingestedTable
    }

    withWatermarks
  }
}

object CustomStreamTableSourceScan{
  private var watermarkStrategy : WatermarkStrategy = null;
  def setWatermarkStrategy(watermarkStrategy : WatermarkStrategy): Unit ={
    this.watermarkStrategy = watermarkStrategy
  }
  def getWatermarkStrategy:WatermarkStrategy = this.watermarkStrategy
}

/**
  * Generates periodic watermarks based on a [[PeriodicWatermarkAssigner]].
  *
  * @param timeFieldIdx the index of the rowtime attribute.
  * @param assigner the watermark assigner.
  */
private class CustomPeriodicWatermarkAssignerWrapper(
    timeFieldIdx: Int,
    assigner: PeriodicWatermarkAssigner)
  extends AssignerWithPeriodicWatermarks[CRow] {

  override def getCurrentWatermark: Watermark = assigner.getWatermark

  override def extractTimestamp(crow: CRow, previousElementTimestamp: Long): Long = {
    val timestamp: Long = crow.row.getField(timeFieldIdx).asInstanceOf[Long]
    assigner.nextTimestamp(timestamp)
    0L
  }
}

/**
  * Generates periodic watermarks based on a [[PunctuatedWatermarkAssigner]].
  *
  * @param timeFieldIdx the index of the rowtime attribute.
  * @param assigner the watermark assigner.
  */
private class CustomPunctuatedWatermarkAssignerWrapper(
    timeFieldIdx: Int,
    assigner: PunctuatedWatermarkAssigner)
  extends AssignerWithPunctuatedWatermarks[CRow] {

  override def checkAndGetNextWatermark(crow: CRow, ts: Long): Watermark = {
    val timestamp: Long = crow.row.getField(timeFieldIdx).asInstanceOf[Long]
    assigner.getWatermark(crow.row, timestamp)
  }

  override def extractTimestamp(element: CRow, previousElementTimestamp: Long): Long = {
    0L
  }
}
