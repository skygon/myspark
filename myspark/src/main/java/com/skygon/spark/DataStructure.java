package com.skygon.spark;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
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
				.setMaster("spark://10.197.38.95:7077");
				//.setMaster("local[*]");
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

	public static void main( String[] args ) throws InterruptedException{
		DataStructure ds = new DataStructure();
		
		Dataset<Row> df = ds.readFromFile("src/resource/JsonlargeFile.json", FileType.JSON);
		Dataset<Row> df2 = ds.readFromFile("src/resource/people.json", FileType.JSON);
		df.show();
		Dataset<Row> df3 = df.select("email");
		System.out.println("start to count lines...");
		System.out.println(df3.count());
		//df.explain();
		System.out.println(ds.mSparkSession.conf().getAll());

		int i = 0;
		String[] col = df.columns();
		List<RDD<Row>> dfs = new LinkedList<RDD<Row>>();
		while (true){
			TimeUnit.SECONDS.sleep(5);
			dfs.add(df.select(col[i]).rdd());
			i ++;
			if (i >= col.length){
				i = 0;
				dfs.get(0).count();
			}
		}
	}
}
