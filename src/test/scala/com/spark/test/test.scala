package com.spark.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val utils = new kudu_Utils
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TestKuduDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    val order_info = utils.get_tables("order_info")
    order_info.createOrReplaceTempView("order_info")

    val shop_info = utils.get_tables("shop_info")
    shop_info.createOrReplaceTempView("shop_info")

    val area = utils.get_tables("area")
    area.createOrReplaceTempView("area")

    val city = utils.get_tables("city")
    city.createOrReplaceTempView("city")

    val province = utils.get_tables("province")
    province.createOrReplaceTempView("province")
    val sql = "SELECT ao.YMD, p.PROVINCE, c.CITY, a.AREA, s.SHOP_NAME , IFNULL(ao.AMOUNT_SUM, 0) AS VALID_ORDER_PRICE, IFNULL(ao.REAL_AMOUNT_SUM, 0) AS REAL_AMOUNT, IFNULL(ao.eb, 0) AS eb FROM (SELECT o.STORE_SEQ, o.YMD, SUM(o.AMOUNT) * 0.5 AS AMOUNT_SUM, SUM(o.REAL_AMOUNT + o.BILL_TIME_PRECHARGE) * 0.5 AS REAL_AMOUNT_SUM, SUM(o.BILL_TIME_PRECHARGE) * 0.5 AS eb FROM ( SELECT PICKUP_STORE_SEQ AS STORE_SEQ, SUBSTR(RETURNDATETIME, 1, 8) AS YMD, AMOUNT, REAL_AMOUNT, BILL_TIME_PRECHARGE FROM order_info WHERE PAYMENT_STATUS = 6 AND RETURNDATETIME IS NOT NULL UNION ALL SELECT RETURN_STORE_SEQ AS STORE_SEQ, SUBSTR(RETURNDATETIME, 1, 8) AS YMD, AMOUNT, REAL_AMOUNT, BILL_TIME_PRECHARGE FROM order_info WHERE PAYMENT_STATUS = 6 AND RETURNDATETIME IS NOT NULL ) o GROUP BY o.STORE_SEQ, o.YMD ) ao LEFT JOIN shop_info s ON ao.STORE_SEQ = s.SHOP_SEQ LEFT JOIN area a ON s.AREA_CODE = a.AREAID LEFT JOIN city c ON a.FATHERID = c.CITYID LEFT JOIN province p ON c.FATHERID = p.PROVINCEID WHERE ao.YMD >= '20170717' AND ao.YMD <= '20170718'"
    spark.sql(sql).show()





  }

}
