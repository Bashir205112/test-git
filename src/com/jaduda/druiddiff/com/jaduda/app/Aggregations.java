package com.jaduda.app;

public abstract class Aggregations {
	private String type;
	private String name;
	
	
	public Aggregations(String type,String outputName) {
		this.type = type;
		this.name = outputName;
	}
	public static class Count extends Aggregations {

		public Count(String type, String outputName) {
			super(type, outputName);
		}
	}
	public static class LonsSum extends Aggregations{
		private String fieldName;

		public LonsSum(String name,String fieldName) {
			super("longSum", name);
			this.fieldName=fieldName;
		}
		
	}
	public static class DoubleSum extends Aggregations{
		private String fieldName;

		public DoubleSum(String type, String outputName,String fieldName) {
			super(type, outputName);
			this.fieldName=fieldName;
			
		}
		
	}
	public static class FloatSum extends Aggregations{
		private String fieldName;

		public FloatSum(String type, String outputName,String fieldName) {
			super(type, outputName);
			this.fieldName=fieldName;
			
		}
		
	}
	public static class DoubleMin extends Aggregations{
		private String fieldName;

		public DoubleMin(String type, String outputName,String fieldName) {
			super(type, fieldName);
			this.fieldName=fieldName;
			
		}
		
	}
	public static class DoubleMax extends Aggregations{
		private String fieldName;

		public DoubleMax(String type, String outputName,String fieldName) {
			super(type, fieldName);
			this.fieldName=fieldName;
			
		}
		
	}
	
	
	
	

}
