package com.jaduda.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.jsoniter.JsonIterator;
import com.jsoniter.annotation.JsonProperty;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class DruidQuery {
	
	private interface QueryCondition {

		void test(DruidQuery druidQuery) throws InvalidQueryException;
		
	}
	
	public static void main(String[] args) throws UnirestException, InvalidQueryException {
		// the time
		Instant before= Instant.now().minus(Duration.ofDays(7L));
		Instant sevenDaysBefore= before.minus(Duration.ofDays(7L));
		
		
		// url
		String url = "http://druid-broker.splicky.com:8083/druid/v2/";
		
		// querys
		DruidQuery queryBefore = new DruidQuery("groupBy").aggregations(new Aggregations.LonsSum("impressions","impressions")).
				filter(new DruidFilter.Selector("country", "CHE"))
				.from("splicky")
				.granularity("ALL")
				.dimensions(new DimensionSpec.DefaultDimensionSpec("bundle", "bundle", "String"))
				.intervals(before,Instant.now());
		
		DruidQuery querySevenDaysBefore = new DruidQuery("groupBy").aggregations(new Aggregations.LonsSum("impressions","impressions")).
				filter(new DruidFilter.Selector("country", "CHE"))
				.from("splicky")
				.granularity("ALL")
				.dimensions(new DimensionSpec.DefaultDimensionSpec("bundle", "bundle", "String"))
				.intervals(sevenDaysBefore,before);
		
		
		// Responses
		HttpResponse<String> responseBefore= Unirest.post(url)
		.header("Content-Type", "application/json")
		.header("Accept", "application/json")
		.body(queryBefore.build())
		.asString();
		
		HttpResponse<String> responseDaysBefore= Unirest.post(url)
				.header("Content-Type", "application/json")
				.header("Accept", "application/json")
				.body(querySevenDaysBefore.build())
				.asString();
		
		// Ausgabe
//		System.out.println(responseBefore.getBody());
//		System.out.println(responseDaysBefore.getBody());
		
		File outputFile = new File("/home/bashir/workspace/jsonIterator/output/output.csv");
		BufferedWriter writer;
		try {
			writer = Files.newBufferedWriter(Paths.get(outputFile.getAbsolutePath()),StandardOpenOption.CREATE);
			DruidQuery.writeCleanLine(responseBefore, responseDaysBefore, writer, outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		

		
//		Any resultBefore = JsonIterator.deserialize(responseBefore.getBody()).get('*', "event");
//		Any resultDaysBefore = JsonIterator.deserialize(responseDaysBefore.getBody()).get('*', "event");
//		
//		File outputFile = new File("/home/bashir/workspace/jsonIterator/output/result.csv");
//		try {
//			BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile.getAbsolutePath()),StandardOpenOption.CREATE);
//			writer.append("Publisher,");
//			writer.append("Impressions im Zeitraum 1,");
//			writer.append("Impressions im Zeitraum 2,");
//			writer.append("Differenz").append(System.lineSeparator());
//			
//			Map<String,Integer> map=new HashMap<String,Integer>();  
//			for(Object obj: resultDaysBefore) {
//				String publisher= String.valueOf(JsonIterator.deserialize(obj.toString()).get("publisher"));
//				Integer impressions=JsonIterator.deserialize(obj.toString()).get("impressions").toInt();
//				System.out.println(publisher);
//				map.put(publisher, impressions);
//			}
//			for(Object obj:resultBefore) {
//				Any publisher= JsonIterator.deserialize(obj.toString()).get("publisher");
//				int impressions= JsonIterator.deserialize(obj.toString()).get("impressions").toInt();
//				writer.append(publisher+",");
//				writer.append(impressions +",");
//				writer.append(map.get(publisher.toString()).toString()+ ",");
//				int impressions2 = map.get(publisher.toString());
//				int result = impressions - impressions2;
//				writer.append(String.valueOf(result)).append(System.lineSeparator());
//				
//			}
//			writer.close();
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	@JsonProperty("queryType")
	private String queryType = "";
	@JsonProperty("dataSource")
	private String dataSource;
	@JsonProperty("granularity")
	private String granularity;
	@JsonProperty("intervals")
	private List<String> intervals;
	
	@JsonProperty("filter")
	private DruidFilter filter;
	
	@JsonProperty("dimensions")
	private List<DimensionSpec> dimensions;
	
	@JsonProperty("aggregations")
	private List<Aggregations> aggregations;

	private static List<QueryCondition> conditions = new ArrayList<>();
	static {
		conditions.add(query -> {
			if(!Objects.equals(query.queryType, "groupBy")) {
				throw new InvalidQueryException("Unknown query type: " + query.queryType);
			}
		});
		conditions.add(query -> checkPresence("datasource", query.dataSource));
		conditions.add(query -> checkPresence("granularity", query.granularity));
		conditions.add(query -> checkPresence("intervals", query.intervals));
	}
	
	public DruidQuery(String groupBy) {
		this.queryType = groupBy;
	}
	
	private static void checkPresence(String field, Object o) throws InvalidQueryException {
		if(o == null) {
			throw new InvalidQueryException("Field " + field + " is required but was null") ; 
		}
	}

	public String getQueryType() {
		 return this.queryType;
	}
	
	public DruidQuery from(String datasource) {
		this.dataSource = datasource;
		return this;
	}
	
	public String getDataSource() {
		return this.dataSource;
	}

	public String build()throws InvalidQueryException {
		for(QueryCondition condition : conditions) {
			condition.test(this);
		}		 
		 
		String result = JsonStream.serialize(this);
//		System.out.println(result);
		return result;
	}
	
	public DruidQuery granularity(String granularity) {
		 this.granularity = granularity;
		 return this;
	}

	public DruidQuery intervals(List<String> intervals) {
		this.intervals = intervals;
		return this;
	}

	public List<String> getIntervals() {
		return intervals;
	}
	public DruidQuery intervals(Instant instantOne,Instant instantTwo) {
		this.intervals = new ArrayList<String>();
		this.intervals.add(instantOne.toString() + "/"+ instantTwo.toString());
		return this;
	}

	public DruidQuery filter(DruidFilter filter) {
		this.filter = filter;
		return this;
		
	}
	public DruidQuery dimensions(DimensionSpec dimensionSpec) {
		this.dimensions = new ArrayList<DimensionSpec>();
		this.dimensions.add(dimensionSpec);
		return this;
	}
	public DruidQuery aggregations(Aggregations aggregations) {
		this.aggregations = new ArrayList<Aggregations>();
		this.aggregations.add(aggregations);
		return this;
	}
	public static void writeCleanLine(HttpResponse<String>  responseBefore,HttpResponse<String> responseDaysBefore,BufferedWriter writer,File outputFile) {
		Any resultBefore = JsonIterator.deserialize(responseBefore.getBody()).get('*', "event");
		Any resultDaysBefore = JsonIterator.deserialize(responseDaysBefore.getBody()).get('*', "event");
		try {
			writer = Files.newBufferedWriter(Paths.get(outputFile.getAbsolutePath()),StandardOpenOption.CREATE);
			writer.append("Publisher,");
			writer.append("Impressions im Zeitraum 1,");
			writer.append("Impressions im Zeitraum 2,");
			writer.append("Differenz").append(System.lineSeparator());
			
			Map<String,Integer> map=new HashMap<String,Integer>();  
			for(Object obj: resultDaysBefore) {
				String publisher= String.valueOf(JsonIterator.deserialize(obj.toString()).get("publisher"));
				Integer impressions=JsonIterator.deserialize(obj.toString()).get("impressions").toInt();
//				System.out.println(publisher);
				map.put(publisher, impressions);
			}
			for(Object obj:resultBefore) {
				Any publisher= JsonIterator.deserialize(obj.toString()).get("publisher");
				int impressions= JsonIterator.deserialize(obj.toString()).get("impressions").toInt();
				writer.append(publisher+",");
				writer.append(impressions +",");
				writer.append(map.get(publisher.toString()).toString()+ ",");
				int impressions2 = map.get(publisher.toString());
				int result = impressions - impressions2;
				writer.append(String.valueOf(result)).append(System.lineSeparator());
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				writer.close();
				if(outputFile.exists()) {
					String fileName = Paths.get(outputFile.getAbsolutePath()).toString();
					System.out.println("the file " + fileName.substring(fileName.lastIndexOf((char)47)+1)+ " is created.");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
