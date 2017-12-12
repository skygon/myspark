package com.skygon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
	
enum FileType {CSV, JSON};

public class DataStructure {
	public SparkSession mSparkSession = null;	
	
	DataStructure(){}
	
	public Dataset<Row> readFromFile(SparkSession sp, String inFile, FileType ft){
		switch(ft){
		case CSV:
			return sp.read().csv(inFile);
		case JSON:
			return sp.read().json(inFile);
		default:
			return null;
		}
	}

	public static void main( String[] args ){
		DataStructure ds = new DataStructure();
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaSparkPi")
				.setMaster("local[*]");
		SparkSession sp = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
		
		Dataset<Row> df = ds.readFromFile(sp, "src/resource/people.json", FileType.JSON);
		df.show();
	}
}
