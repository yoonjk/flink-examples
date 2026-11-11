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

public class RightJoinExample {
	public static void main(String[] args) throws Exception {

		// 1. Execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// CSV files are bounded inputs
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.setParallelism(1);
		
		// 2. Parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		// make parameters available in the web interface

		String input1 = params.getRequired("input1");
		String input2 = params.getRequired("input2");
		String output = params.getRequired("output");
		
		
		env.getConfig().setGlobalJobParameters(params);

		// 3. Person FileSource
		FileSource<String> personSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input1)
			).build();
		
		// 4. Location FileSource
		FileSource<String> locationSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input2)
			).build();
		
		// 5. Person CSV -> Tuple2
		DataStream<Tuple2<Integer, String>> personSet = env.fromSource(
				personSource, 
				WatermarkStrategy.noWatermarks(), 
				"person Source").map(new CsvToTuple())
				.name("Person Mapper");
		
		// 6. Location CSV -> Tuple2
		DataStream<Tuple2<Integer, String>> locationSet = env.fromSource(
				locationSource, 
				WatermarkStrategy.noWatermarks(),
				"location Source"
			).map(new CsvToTuple())
				.name("Location Mapper");
		
		// 7. DataStream -> Table
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);		
		
		tableEnv.createTemporaryView("persons", personSet);
		tableEnv.createTemporaryView("locations", locationSet);
		
		// 8. RIGHT OUTER JOIN
        // Preserve all records from locations
        Table joinedTable = tableEnv.sqlQuery(
            "SELECT "
          + " l.f0 AS person_id, "
          + " COALESCE(p.f1, 'NULL') AS person_name, "
          + " l.f1 AS state "
          + "FROM persons AS p "
          + "RIGHT OUTER JOIN locations AS l "
          + "ON p.f0 = l.f0"
        );
        
        // 9. Table -> DataStream
        DataStream <Row> joinedRows = tableEnv.toDataStream(joinedTable);
        
        // 10. MapFunction: Row -> String
        DataStream <String> csvOutput = joinedRows
        		.map(new JoinResultMapper())
        		.name("JOIN Result Mapper");
        
        // 11. Output file configuration
		OutputFileConfig outputConfig = 
				OutputFileConfig.builder()
					.withPartPrefix("right-join")
					.withPartSuffix(".csv")
					.build();
		
		// 12. FileSink
		FileSink fileSink = FileSink.forRowFormat(
				new Path(output),
				new SimpleStringEncoder<String>("UTF-8")
				)
				.withBucketAssigner(
						new BasePathBucketAssigner<String>()
				)
				.withOutputFileConfig(outputConfig)
				.build();
				
		csvOutput
			.sinkTo(fileSink)
			.name("Right Join File Sink");
		
        // Debug output
        csvOutput.print("RIGHT-JOIN");
        
        // 13. Execute
        env.execute("Right Outer Join Example");        
	}

}
