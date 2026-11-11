package com.demo.example.join;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.ParameterTool;

public class LefOuterJoinExample {
	public static void main(String[] args) throws Exception {

//  	set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.setParallelism(1);
		
        // 2. Parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
		String input1 = params.getRequired("input1");
		String input2 = params.getRequired("input2");
		String output = params.getRequired("output");
		
		// 3. Person FileSource
		FileSource<String> personSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input1)
				).build();
		
		FileSource<String> locationSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input2)
				).build();
		
		DataStream<Tuple2<Integer, String>> personSet = env.fromSource(
				personSource, 
				WatermarkStrategy.noWatermarks(), 
				"Person Source").map(new CsvToTuple());
				

		DataStream<Tuple2<Integer, String>> locationSet = env.fromSource(
				locationSource, 
				WatermarkStrategy.noWatermarks(),
				"location Source").map(new CsvToTuple());

		// 7. DataStream -> Table
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// 8. LEFT OUTER JOIN
		tableEnv.createTemporaryView("persons", personSet);
		tableEnv.createTemporaryView("locations", locationSet);

		// 9. Table -> DataStream
		Table joinedTable = tableEnv.sqlQuery(
	            "SELECT " +
	                    "  p.f0 AS person_id, " +
	                    "  p.f1 AS person_name, " +
	                    "  COALESCE(l.f1, 'NULL') AS state " +
	                    "FROM persons AS p " +
	                    "LEFT JOIN locations AS l " +
	                    "ON p.f0 = l.f0"
	                );
		DataStream<Row> joinedRows = tableEnv.toDataStream(joinedTable);
		
		// 10. MapFunction: Row -> CSV
		DataStream<String> csvResult = joinedRows.map(new JoinResultMapper());
		
		// 11. FileSink
		OutputFileConfig outputConfig = 
				OutputFileConfig.builder()
				.withPartPrefix("left-join")
				.withPartSuffix(".csv")
				.build();
		
		FileSink<String> sink =
				FileSink.forRowFormat(
						new Path(output), 
						new SimpleStringEncoder<String>("UTF-8")
				).withBucketAssigner(
					new BasePathBucketAssigner<String>()	
				).withOutputFileConfig(outputConfig)
				.build();
		
		csvResult.sinkTo(sink).name("LEFT JOIN CSV Sink");
		
        // Debug output
        csvResult.print("LEFT-JOIN");

		env.execute("Left Outer Join Example");
	}
}
