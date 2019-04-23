package com.jaduda.app;

import java.util.Collection;

public abstract class DruidFilter {
	
	private String type;
	private String dimension;
	
	public DruidFilter(String type,String dimension) {
		this.type =type;
		this.dimension = dimension;
	}
	public DruidFilter(String type) {
		this.type =type;
	}
	
	
	
	public static class In extends DruidFilter {
		private Collection<String> values;

		public In(String dimension, Collection<String> values) throws InvalidQueryException {
			super("in", dimension);
			if(values.size() == 0) {
				throw new InvalidQueryException("the values object is null");
			}else {
				this.values = values;
			}
		}
	}
	
	public static class Selector extends DruidFilter {
		private String value;

		public Selector (String dimension, String value) {
			super("selector", dimension);
			if(value == null) {
				System.out.println("the value is null");
			}else {
				this.value = value;	
			}
			
		}
	}
	
	public static class And extends DruidFilter {
		private Collection<DruidFilter> fields;

		public And(Collection<DruidFilter> fields)throws InvalidQueryException {
			super("and");
			if(fields.size() == 0) {
				throw new InvalidQueryException("the fields object is null");
			}else {
				this.fields = fields;
			}
			
		}
	}
	public static class Or extends DruidFilter {
		private Collection<DruidFilter> fields;

		public Or(Collection<DruidFilter> fields)throws InvalidQueryException {
			super("or");
			if(fields.size() == 0) {
				throw new InvalidQueryException("the fields object is null");
			}else {
				this.fields = fields;
			}
		}
	}
	public static class Not extends DruidFilter {
		private DruidFilter field;

		public Not(DruidFilter field) throws InvalidQueryException{
			super("not");
			if(field == null) {
				throw new InvalidQueryException("the field is null");
			}else {
				this.field = field;	
			}
			
		}
	}
	

	

	
	

}
