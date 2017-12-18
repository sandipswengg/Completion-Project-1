import java.io.FileNotFoundException
import java.io.IOException

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 

object CallDrop {
  def main(args : Array[String])
  {
  
    var error_code:String = "";
    var filepath:String = ""; 
    var outputPath:String = "";
    
    // Accept the param from the command line arguments
    try {
      error_code = args(0);
      filepath = args(1);
      outputPath = args(2);  
    }
    catch {
      case aIOB: ArrayIndexOutOfBoundsException => {
        println("Argument list does not match with the program!");
      }
    }
    
    
    // Generate the SparkSession
    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("CallDrop")
      .getOrCreate() 
      
    try {
      // Read the context of the file and prepare the data frame
      val df = sparkSession.read.csv(filepath).cache()
      
      val listDataDF = df
      .filter( x => x(3).equals(error_code))
      .groupBy("_c2")
      .count()
      .select(col("_c2").alias("MOB"), col("count").alias("CALL_DROP_COUNT"))
      .toDF("MOB", "Count")
      
      // Temp view of the Dataframe in SQL Context
      listDataDF.createOrReplaceTempView("mobData")
      val sqlDF = sparkSession.sql("SELECT * FROM mobData ORDER BY Count DESC LIMIT 10")
      
      sqlDF.write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv").option("header", true).save(outputPath)
      
      sqlDF.show()

    } 
    catch {
      case ioE: IOException => {
        println("IO Exception loading file!")
      }
      case fnotFoundE: FileNotFoundException => {
        println("File ${filepath} not found!" + fnotFoundE.printStackTrace())
      }
    }

  }
}