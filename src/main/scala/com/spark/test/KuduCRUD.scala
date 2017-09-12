package com.xieenze.kudutst

/**
  * Created by xieenze on 2017/9/5.
  */

import org.apache.kudu.Type
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.log4j.{Level, Logger}

/**
  * Spark on Kudu 的案例
  *
  * 使用spark对于kudu表 展示 insert, update and scan 操作.并且可以修改表结构，如增加一列等
  * 主要使用spark集成的kuduContext和kudu自带的kuduClient两个客户端
  *
  */
object kuduCRUD_demo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class Customer(name:String, age:Int, city:String)
  val spark = SparkSession.builder
    .appName("TestKuduDF")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  import spark.implicits._

  val kuduMasters = "10.64.217.195:7051"
  val kuduContext = new KuduContext(kuduMasters,sc)
  val kuduTableName = "tj_student"
  val kuduOptions = Map("kudu.master" -> "10.64.217.195:7051",
    "kudu.table" -> kuduTableName,
    "kudu.operation.timeout.ms" -> "10000")

  val customers = Array(
    Customer("jane", 30, "new york"),
    Customer("jordan1111111111111111", 18, "toronto"),
    Customer("enzo2222222222222222", 43, "oakland"),
    Customer("laura33333333333333333333", 27, "vancouver"))
  val customersRDD = sc.parallelize(customers)
  val customersDF = customersRDD.toDF()

  val client = new KuduClient.KuduClientBuilder(kuduMasters).build()


  def main(args: Array[String]): Unit = {
    //deleteColumns()
    //addColumms()
    //renameColumms()
  }

  //创建表
  def createTable(kuduTableName:String) ={
    //1 定义表结构
    //两种方式 1 通过现成的datafram创建表，2 自己定义表结构
    val kuduTableSchema = customersDF.schema
    /*val kuduTableSchema = StructType(
          StructField(name="name",dataType=StringType,nullable=false)::
            StructField("age" , IntegerType, true ) ::
            StructField("city", StringType , true ) :: Nil)*/
    //2 定义主键
    val kuduTablePrimaryKey = Seq("name")
    //3 定义一些其他的配置信息
    //这里使用了kudu的java client，调用了java的方法，所以要asjava，转成java类型
    //表的复制和分区选项
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.
      setRangePartitionColumns(List("name").asJava).//设置范围分区的列
      setNumReplicas(1).// 设置副本数量
      addHashPartitions(List("name").asJava,20)//设置分区 数据分到几台机器上
    //4 创建表
    kuduContext.createTable(kuduTableName,kuduTableSchema,kuduTablePrimaryKey,kuduTableOptions)
  }
  //删除表
  def deleteTable(kuduTableName:String): Unit ={
    kuduContext.deleteTable(kuduTableName)
  }
  //判断表是否存在
  def isTableExists(kuduTableName:String): Unit ={
    val flag = kuduContext.tableExists(kuduTableName)
    flag
  }
  //插入数据
  def insertRowsToTableUseKuduAPI(kuduTableName:String,dataDF:DataFrame): Unit ={
    kuduContext.insertRows(dataDF,kuduTableName)
  }
  //增加一列，通过kuduClient操作，详情参考https://kudu.apache.org/apidocs/org/apache/kudu/client/KuduClient.html
  def addColumms(): Unit ={
    val ato = new AlterTableOptions
    ato.addColumn("height",Type.STRING,"170")
    client.alterTable(kuduTableName,ato)
  }
  //删除一列
  def deleteColumns(): Unit ={
    val ato = new AlterTableOptions
    ato.dropColumn("height")
    client.alterTable(kuduTableName,ato)
  }
  //重命名一列
  def renameColumms(): Unit ={
    val ato = new AlterTableOptions
    ato.renameColumn("height","heightttt")
    client.alterTable(kuduTableName,ato)
  }
  //使用sparkSQL读取数据，返回DataFrame
  def readDataFromTableUseDataFrame() ={
    val customerReadDF = spark.read.options(kuduOptions).kudu
    customerReadDF
  }

  /*
  剩下的看这2位老哥的代码吧，懒得写了，没啥难度
  https://github.com/mladkov/spark-kudu-up-and-running/blob/master/src/main/scala/com/cloudera/examples/spark/kudu/SparkKuduUpAndRunning.scala
  https://github.com/jimmy-src/kudu-learning/blob/d5330ca3d725c3a47997e0d1adc321cc7afec65c/src/main/KuduSparkIntergration/KuduSpark.scala
   */
  def deleteRowsFromTable(): Unit ={
    //TODO
  }
  def upsertToTable(): Unit ={
    //TODO
  }
  def updateToTable(): Unit ={
    //TODO
  }
  def insertToTableUseDataFrameAPI(): Unit ={
    //TODO
  }
  def insertToTableUseSparkSQL(): Unit ={
    //TODO
  }
  //谓词下推 查询
  def selectUsePredicatePushdown(): Unit ={
    //TODO
  }

}
