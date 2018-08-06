package spark01.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

/**
  * create by nulijiushimeili on 2018-08-06
  */
object AvgUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    // 指定UDAF函数的输入参数类型 schema
    StructType(Array(
      StructField("iv", DoubleType)
    ))
  }

  override def bufferSchema: StructType = {
    // 给定参数的缓存类型; avg = totalValue / totalCount
    StructType(Array(
      StructField("tv", DoubleType),
      StructField("tc", IntegerType)
    ))
  }

  override def dataType: DataType = {
    // 给定返回的数据类型
    DoubleType
  }

  override def deterministic: Boolean = {
    // 给定多次运行是否允许返回结果不一致(模糊查询),true表示不允许,即必须返回一个确定值,false表示允许
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 初始化缓存数据的初始值
    buffer.update(0, 0.0)
    buffer.update(1, 0.0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 对于每一条输入数据(当前分组的),更新buffer的值
    // 1. 获取输入数据
    val iv = input.getDouble(0)
    // 2. 获取缓存区数据
    val tv = buffer.getDouble(0)
    val tc = buffer.getInt(1)
    // 3. 更新缓存区数据
    buffer.update(0, tv + iv)
    buffer.update(1, tc + 1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 当两个分区的结果需要进行合并的时候,会调用该merge方法
    // 1. 获取buffer1的数据
    val tv1 = buffer1.getDouble(0)
    val tc1 = buffer1.getInt(1)
    // 2. 获取buffer2的数据
    val tv2 = buffer2.getDouble(0)
    val tc2 = buffer2.getInt(1)
    // 3. 更新buffer1的数据
    buffer1.update(0, tv1 + tv2)
    buffer1.update(1, tc1 + tc2)
  }

  override def evaluate(buffer: Row): Any = {
    // 计算最终结果
    buffer.getDouble(0) / buffer.getInt(1)
  }
}
