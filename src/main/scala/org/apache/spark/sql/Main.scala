package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.util.ArrowUtils

import java.nio.channels.Channels
import java.util.Arrays.asList


object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    val vectorSchemaRoot = createVectorSchemaRoot

    println(vectorSchemaRoot.contentToTSVString())

    val out = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))
    writer.start()
    writer.writeBatch()
    writer.end()
    writer.close()


    val in = new ByteArrayReadableSeekableByteChannel(out.toByteArray)
    val sparkSchema = ArrowUtils.fromArrowSchema(vectorSchemaRoot.getSchema)
    val shit = ArrowConverters.getBatchesFromStream(in)
    val df = ArrowConverters.toDataFrame(spark.sparkContext.parallelize(shit.toSeq), sparkSchema.json, spark)
    df.show()

  }

  private def createVectorSchemaRoot: VectorSchemaRoot = {

    val name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    val age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)

    val schema = new Schema(asList(name, age))

    val allocator = new RootAllocator()
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

    val nameVector = vectorSchemaRoot.getVector("name").asInstanceOf[VarCharVector]
    nameVector.allocateNew(3)
    nameVector.set(0, "David".getBytes)
    nameVector.set(1, "Gladis".getBytes)
    nameVector.set(2, "Juan".getBytes)
    nameVector.setValueCount(3)

    val ageVector = vectorSchemaRoot.getVector("age").asInstanceOf[IntVector]
    ageVector.allocateNew(3)
    ageVector.set(0, 10)
    ageVector.set(1, 20)
    ageVector.set(2, 30)
    ageVector.setValueCount(3)

    vectorSchemaRoot.setRowCount(3)
    vectorSchemaRoot
  }
}
