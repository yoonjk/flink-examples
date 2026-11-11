package com.demo.example.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class JoinResultMapper implements MapFunction<Row, String> {

	@Override
	public String map(Row row) {

		return row.getField(0) + "," + row.getField(1) + "," + row.getField(2);
	}
}
