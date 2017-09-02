package com.spark.test

/**
  * Created by xieenze on 2017/9/1.
  */
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
  * 使用spark对于kudu表 展示 insert, update and scan 操作.
  *
  */

object test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class Customer(name:String, age:Int, city:String)
  def main(args: Array[String]): Unit = {

    // Setup Spark configuration and related contexts
    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("Spark Kudu Up and Running Example").
      set("spark.driver.allowMultipleContexts","true")
    // Create a Spark and SQL context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)



    // Comma-separated list of Kudu masters with port numbers
    val kuduMasters = "10.64.217.195:7051"
    // Create an instance of a KuduContext
    val kuduContext = new KuduContext(kuduMasters,sc)
    // This allows us to implicitly convert RDD to DataFrame
    import sqlContext.implicits._

    // Specify a table name
    var kuduTableName = "spark_kudu_tbl"

    // Define Kudu options used by various operations
    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)


    // Check if the table exists, and drop it if it does
    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }

    ///-------------------------------------------------------------------------------------
    /// 创建表
    // 1. Give your table a name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define a schema
    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("name", StringType , false) ::
        StructField("age" , IntegerType, true ) ::
        StructField("city", StringType , true ) :: Nil)

    // 3. Define the primary key
    val kuduPrimaryKey = Seq("name")

    // 4. Specify any further options
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.
      setRangePartitionColumns(List("name").asJava).
      setNumReplicas(1)

    // 5. Call create table API
    kuduContext.createTable(
      // Table name, schema, primary key and options
      kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)

    /// WRITING TO TABLE - KUDU CONTEXT
    // Define a list of customers based on the case class already defined above
    val customers = Array(
      Customer("jane", 30, "new york"),
      Customer("jordan", 18, "toronto"))

    // Create RDD out of the customers Array
    val customersRDD = sc.parallelize(customers)

    // Now, using reflection, this RDD can easily be converted to a DataFrame
    // Ensure to do the :
    //     import sqlContext.implicits._
    // above to have the toDF() function available to you
    val customersDF = customersRDD.toDF()


    // 5. Read data from Kudu table
    sqlContext.read.options(kuduOptions).kudu.show
    ///-------------------------------------------------------------------------------------


    ///-------------------------------------------------------------------------------------
    /// 插入数据

    // 1. 指定kudu表
    kuduTableName = "spark_kudu_tbl"

    // 2. 把dataFrame格式的数据插入kudu表
    kuduContext.insertRows(customersDF, kuduTableName)

    // 3. 从kudu中读取记录并显示
    sqlContext.read.options(kuduOptions).kudu.show
    ///-------------------------------------------------------------------------------------

  }

  def createTable(kuduTableName:String,kuduContext: KuduContext) ={
    //1 定义表结构
    val kuduTableSchema = StructType(
      StructField("name",StringType,false)::
      StructField("age" , IntegerType, true ) ::
      StructField("city", StringType , true ) :: Nil
    )
    //2 定义主键
    val kuduTablePrimaryKey = Seq("name")
    //3 定义一些其他的配置信息
    //这里使用了kudu的java client，调用了java的方法，所以要asjava，转成java类型
    //表的复制和分区选项
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.
      //暂时不明白干嘛的
      setRangePartitionColumns(List("name").asJava).
      //设置分区 数据分到几台机器上
      setNumReplicas(2)
    //4 创建表
    kuduContext.createTable(kuduTableName,kuduTableSchema,kuduTablePrimaryKey,kuduTableOptions)
  }
  def deleteTable(kuduTableName:String,kuduContext: KuduContext): Unit ={
    kuduContext.deleteTable(kuduTableName)
  }
  def isTableExists(kuduTableName:String,kuduContext: KuduContext): Unit ={
    val flag = kuduContext.tableExists(kuduTableName)
    flag
  }
  def insertRowsToTableUseKuduAPI(kuduTableName:String,kuduContext:KuduContext,dataDF:DataFrame): Unit ={
    kuduContext.insertRows(dataDF,kuduTableName)
  }
  def deleteRowsFromTable(): Unit ={
    //TODO
    //https://github.com/mladkov/spark-kudu-up-and-running/blob/master/src/main/scala/com/cloudera/examples/spark/kudu/SparkKuduUpAndRunning.scala#L48
  }
  def upsertToTable(): Unit ={
    //TODO
  }
  def updateToTable(): Unit ={
    //TODO
  }
  //使用kudu api读取数据，返回kuduRDD
  def readDataFromTableUseKuduAPI(sc:SparkContext,kuduTableName:String,kuduTableProjColumns:Seq[String],kuduContext:KuduContext) ={
    val custRDD = kuduContext.kuduRDD(sc, kuduTableName, kuduTableProjColumns)
    val a = custRDD.collect()
    custRDD
  }
  //使用sparkSQL读取数据，返回DataFrame
  def readDataFromTableUseDataFrame(sqlContext: SQLContext,kuduOptions:Map[String,String]) ={
    val customerReadDF = sqlContext.read.options(kuduOptions).kudu
    customerReadDF
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
