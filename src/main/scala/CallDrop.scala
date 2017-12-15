import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CallDrop {
  def main(args : Array[String]) {
    
    /*val sc = new SparkConf()
    .setAppName("Call Drop Checker")
    .setMaster("local");
    
    val sprk_context = new SparkContext(sc); 
    val sqlContext = new SQLContext(sprk_context);
    val csvData = sqlContext.read.csv("CDR.csv").cache()
    
    val spark = SparkSession.builder().getOrCreate() 
    
    // println(csvData.printSchema())
    
    val callDropList = csvData
    .filter( x => x(3).equals("0x829F08"))
    .groupBy("_c3", "_c2")
    .count()
    
    callDropList.select("_c2", "count").show()
    
    */
    
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
      
    val df = sparkSession.read.csv("CDR.csv")
    
    df.printSchema()
    
    val listData = df.filter( x => x(3).equals("0x829F08"))
    .groupBy("_c3", "_c2")
    .count()
    .select(col("_c2").alias("MOB"), col("count").alias("CALL_DROP_COUNT"))
    .toDF("MOB", "Count")
    .show()
 
    println(listData)
  }
}