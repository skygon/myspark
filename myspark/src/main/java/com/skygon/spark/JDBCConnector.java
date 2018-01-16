package com.skygon.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBCConnector {
	private SparkSession mSparkSession = null;
	private Map<String, String> options = new HashMap<String, String>();
	
	JDBCConnector(){
		SparkConf sparkConf = new SparkConf()
				.setAppName("JDBCConnector")
				.setMaster("spark://10.197.38.95:7077");
		
		this.mSparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
				
	}
	
	void setOptions(){
		options.put("url", "jdbc:mysql://10.27.16.129:3306/world");
		options.put("driver", "com.mysql.jdbc.Driver");
		options.put("user", "test");
		options.put("password", "1234");
		options.put("dbtable", "city");
	}
	
	void loadData(){
		setOptions();
		Dataset<Row> df = mSparkSession.read().format("jdbc").options(options).load();
	}
	public static void main(String[] args){
		System.out.println("=== start app ===");
		JDBCConnector connector = new JDBCConnector();
		connector.loadData();
		System.out.println("== Finish load data ===");
	}
}
