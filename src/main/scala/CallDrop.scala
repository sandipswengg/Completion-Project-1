import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession


object CallDrop {
  def main(args : Array[String]) {
    
    val sc = new SparkConf()
    .setAppName("Call Drop Checker")
    .setMaster("local");
    
    val sprk_context = new SparkContext(sc); 
    val sqlContext = new SQLContext(sprk_context);
    val csvData = sqlContext.read.csv("CDR.csv").cache()
    
    // println(csvData.printSchema())
    
    // val callDropList = csvData.rdd.filter { x => x(3).equals("0x829F08") }
    // val callDropList = csvData.groupBy("_c3", "_c2").count()
    val callDropList = csvData
    .filter( x => x(3).equals("0x829F08"))
    .groupBy("_c3", "_c2")
    .count()
    
    println(callDropList.show())
    
  }
    
}