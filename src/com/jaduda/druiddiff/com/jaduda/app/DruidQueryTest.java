package com.jaduda.app;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import com.jaduda.app.DimensionSpec.DefaultDimensionSpec;
import com.jaduda.app.DruidFilter.Selector;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;




public class DruidQueryTest {

	@Test
	public void groupBy_initlization() {
		DruidQuery query = new DruidQuery("groupBy");
		assertEquals("groupBy", query.getQueryType());
	}
	
	@Test
	public void aQuery_acceptsADataSource() {
		DruidQuery query = new DruidQuery("groupBy")
				.from("DATASOURCE");
		assertEquals("DATASOURCE", query.getDataSource());
	}
	
	@Test
	public void aQuery_requiresADatasource() {
		DruidQuery query = new DruidQuery("groupBy");
		try {
			query.build();
			fail();
		} catch(InvalidQueryException e) {
			assertTrue(e.getMessage().contains("datasource"));
		}
	}
	
	@Test
	public void anInvalidQueryType_isRejected() {
		DruidQuery query = new DruidQuery("GARBAGE");
		
		try {
			query.build();
			fail();
		} catch(InvalidQueryException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("Unknown query type: GARBAGE"));
		}
	}
	
	@Test
	public void aQuery_serializesItsFields() throws InvalidQueryException {
		DruidQuery query = new DruidQuery("groupBy").from("DATASOURCE").granularity("ALL").intervals(Instant.now(), Instant.now());
		Any result = JsonIterator.deserialize(query.build());
		assertEquals("ALL", result.get("granularity").toString());
		assertEquals("DATASOURCE", result.get("dataSource").toString());
	}
	
	@Test
	public void aQuery_requiredAGranularity() {
		DruidQuery query = new DruidQuery("groupBy").from("DATASOURCE");
		try {
			query.build();
			fail();
		} catch(InvalidQueryException e) {
			assertTrue(e.getMessage().contains("granularity"));
		}
	}
	@Test
	public void aQuery_requiredAintervals() {
		List<String> intervals = new ArrayList<String>();
		intervals.add("2012-01-01T00:00:00.000/2012-01-03T00:00:00.000");
		DruidQuery query = new DruidQuery("groupBy").granularity("ALL").from("DATASOURCE").intervals(intervals);
		String actual = query.getIntervals().get(0);
		String expected= "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000";
		assertEquals(expected, actual);
	}
	
	@Test
	public void aQuery_receivesIntervalsByInstants() {
		DruidQuery query = new DruidQuery("groupBy")
				.from("DATASOURCE")
				.intervals(Instant.parse("2012-01-01T00:00:00.000Z"), Instant.parse("2012-01-03T00:00:00.000Z"));
		String actual = query.getIntervals().get(0);
		String expected = "2012-01-01T00:00:00Z/2012-01-03T00:00:00Z";
		assertEquals(expected, actual);
	}
	@Test
	public void aQuery_acceptsAintervals() {
		DruidFilter druidFilter= new DruidFilter.Selector("bla bla","trlala");
		Collection<String> values = new ArrayList<String>();
		Collection<DruidFilter> fields = new ArrayList<DruidFilter>();

		fields.add(druidFilter);
		values.add("good");
		List<String> intervals = new ArrayList<String>();
		intervals.add("2012-01-01T00:00:00.000/2012-01-03T00:00:00.000");
		DruidQuery query;
		try {
			query = new DruidQuery("groupBy").
					dimensions(new DimensionSpec.DefaultDimensionSpec("dimension","outputName","String")).
					aggregations(new Aggregations.LonsSum("outputName","metricName" )).
					from("DATASOURCE").
					granularity("ALL").
					intervals(intervals).
					filter(new DruidFilter.In("bla bla",values));
//			filter(new DruidFilter.Not(new DruidFilter.And(fields)));
			Any result = JsonIterator.deserialize(query.build());
			List<String> actual = query.getIntervals();
			List<String> expected = result.get("intervals").asList().stream().map(Any::toString).collect(Collectors.toList());
			assertEquals(expected,actual);
		} catch (InvalidQueryException e1) {
			System.out.println(e1.getMessage());
		}

	}
	
}
