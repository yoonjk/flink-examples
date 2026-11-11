package com.demo.example.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CsvToTuple implements MapFunction<String, Tuple2<Integer, String>> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public Tuple2<Integer, String> map(String value) {

		String[] words = value.split(",", 2);

		return Tuple2.of(Integer.parseInt(words[0].trim()), words[1].trim());
	}
}
