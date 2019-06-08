package com.kt.test1

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[String] = env.fromElements("hello","world","java","hello","java")
    val mapData: DataSet[(String, Int)] = dataSet.map(line => (line,1))


    val groupbyData: GroupedDataSet[(String, Int)] = mapData.groupBy(0)

    val groupBySum: AggregateDataSet[(String, Int)] = groupbyData.sum(1)

    groupBySum.print()



  }



}
