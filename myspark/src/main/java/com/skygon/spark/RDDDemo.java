package com.skygon.spark;

import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RDDDemo {
	public SparkSession mSparkSession = null;
	RDDDemo(){
		SparkConf sparkConf = new SparkConf()
				.setAppName("RDDDemo")
				.setMaster("spark://10.197.38.95:7077");
				//.setMaster("local[*]");
		this.mSparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
	}
	
	public static void main(String[] args) throws InterruptedException{
		RDDDemo demo = new RDDDemo();
		
		Dataset<Row> df = demo.mSparkSession.read().csv("file:///E:/CSV_1517000.csv");
		df.show();
		System.out.println("Total row count: " + df.count());
		df.repartition(10);
		
		while(true){
			TimeUnit.SECONDS.sleep(5);
		}
	}

}
