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

public class OuterJoinExample {
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		// 1. Execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Bounded CSV files - Batch mode
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        
        // 2. Command-line parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
		String input1 = params.getRequired("input1");
		String input2 = params.getRequired("input2");
		String output = params.getRequired("output");
		
		// 3. Person FileSource
		FileSource personSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input1)
			).build();
		
		 // 4. Location FileSource
		FileSource locationSource = FileSource.forRecordStreamFormat(
				new TextLineInputFormat(), 
				new Path(input2)
			).build();

		// 5. Person: String -> Tuple2
		DataStream<Tuple2<Integer, String>> personSet =
				env.fromSource(
						personSource, 
						WatermarkStrategy.noWatermarks(), 
						"person Source").map(new CsvToTuple())
				.name("Person CSV Mapper");
		
		// 6. Location: String -> Tuple2
		DataStream<Tuple2<Integer, String>> locationSet = 
				env.fromSource(
						locationSource, 
						WatermarkStrategy.noWatermarks(), 
						"location Source").map(new CsvToTuple())
				.name("Location CSV Mapper");
		
		// 7. DataStream -> Table
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// 8. FULL OUTER JOIN
		
		tableEnv.createTemporaryView("persons", personSet);
		tableEnv.createTemporaryView("locations", locationSet);
		
		// 8. FULL OUTER JOIN
        Table joinedTable = tableEnv.sqlQuery(
                "SELECT "
              + " COALESCE(p.f0, l.f0) AS person_id, "
              + " COALESCE(p.f1, 'NULL') AS person_name, "
              + " COALESCE(l.f1, 'NULL') AS state "
              + "FROM persons AS p "
              + "FULL OUTER JOIN locations AS l "
              + "ON p.f0 = l.f0"
            );
        
        // 9. Table -> DataStream
        DataStream<Row> joinedRows = tableEnv.toDataStream(joinedTable);
        
        // 10. JOIN result -> CSV
        DataStream<String> csvOutput = joinedRows
        		.map(new JoinResultMapper())
        		.name("JOIN Result Mapper");
        
        // 11. Output file configuration
        OutputFileConfig outputConfig = OutputFileConfig
        		.builder()
        		.withPartPrefix("outer-join")
        		.withPartSuffix(".csv")
        		.build();
        
        
        // 12. FileSink
        FileSink<String> fileSink = 
        		FileSink.forRowFormat(
        			new Path(output),
        			new SimpleStringEncoder<String>("UTF-8")
        		).withBucketAssigner(
                    new BasePathBucketAssigner<String>()
        	    ).withOutputFileConfig(outputConfig)
        		.build();
        
        csvOutput
        	.sinkTo(fileSink)
        	.name("FULL OUTER JOIN CSV Sink");
        
        
        // Debug
        csvOutput.print("OUTER-JOIN");
        
		env.execute("Right Outer Join Example");
	}

}
