package com.skygon.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
	
enum FileType {CSV, JSON};

public class DataStructure {
	public SparkSession mSparkSession = null;	
	DataStructure(){
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaSparkPi")
				.setMaster("local[*]");
		this.mSparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
	}
	
	
	public Dataset<Row> readFromFile(String inFile, FileType ft){
		switch(ft){
		case CSV:
			return this.mSparkSession.read().csv(inFile);
		case JSON:
			return this.mSparkSession.read().json(inFile);
		default:
			return null;
		}
		
	}
	
	public void transform(){
		//RowFactory.create(values)
	}

	public static void main( String[] args ){
		DataStructure ds = new DataStructure();
		
		Dataset<Row> df = ds.readFromFile("src/resource/people.json", FileType.JSON);
		//df.show();
		df.explain();
		System.out.println(ds.mSparkSession.conf().getAll());
		
	}
}
