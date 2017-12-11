package com.skygon.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
	
enum FileType {CSV, JSON};

public class DataStructure {
	private SparkSession mSparkSession = null;	
	
	DataStructure(){
		this.mSparkSession = SparkSession.builder()
				.appName("Spark SQL example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		
	}
	
	public Dataset<Row> readFromFile(String inFile, FileType ft){
		switch(ft){
		case CSV:
			return mSparkSession.read().csv(inFile);
		case JSON:
			return mSparkSession.read().json(inFile);
		default:
			return null;
		}
	}

	public static void main( String[] args ){
		/*DataStructure ds = new DataStructure();
		Dataset<Row> df = ds.readFromFile("resource/people.json", FileType.JSON);
		df.show();*/
	}
}
