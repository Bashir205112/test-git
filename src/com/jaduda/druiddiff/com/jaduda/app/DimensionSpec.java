package com.jaduda.app;



public abstract class DimensionSpec {
	private String type;
	private String dimension;
	private String outputName;
	private String outputType;
	
	
	public DimensionSpec(String type,String dimension,String outputName,String outputType) {
		this.type = type;
		this.dimension  = dimension;
		this.outputName = outputName;
		this.outputType = outputType;
	}
	
	public static class DefaultDimensionSpec extends DimensionSpec {
		public DefaultDimensionSpec(String dimension,String outputName,String outputType)  {
			super("default", dimension,outputName,outputType);

		}
	}
	public static class ExtractionDimensionSpec extends DimensionSpec {
		private String extractionFn;
		public ExtractionDimensionSpec(String dimension,String outputName,String outputType,String extractionFn)  {
			super("extraction", dimension,outputName,outputType);
			this.extractionFn=extractionFn;
		}
	}
	
	

}
