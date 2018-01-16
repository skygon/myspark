package com.skygon.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class JDBCConnector {
	private SparkSession mSparkSession = null;
	private Map<String, String> options = new HashMap<String, String>();
	
	JDBCConnector(){
		SparkConf sparkConf = new SparkConf()
				.setAppName("JDBCConnector")
				.setMaster("spark://10.197.38.95:7077");
				//.setMaster("local[*]");
		
		this.mSparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
		
		// schema is retrieved at driver program. That's why app still works without addjar.
		// But actions on RDD launch at work node and has no context of mysql jdbc driver,
		// that's why we need to add jar files to spark context.
		this.mSparkSession.sparkContext().addJar("file:///E:/Java/project/myspark/myspark/lib/mysql-connector-java-5.1.44-bin.jar");
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
		StructType schema = df.schema();
		df.show();
		System.out.println("schema is " + schema.toString());
	}
	public static void main(String[] args){
		System.out.println("=== start app ===");
		JDBCConnector connector = new JDBCConnector();
		connector.loadData();
		System.out.println("== Finish load data ===");
	}
}
