package com.demo.example.datastreamapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

public class Aggregation {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
        // 실습용: 유한 파일 입력을 배치 모드로 처리
        env.setRuntimeMode(
                org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        
        env.setParallelism(1);
        
        ParameterTool params = ParameterTool.fromArgs(args);
        
        String input = params.get("input");
        String outputBase = params.get("output");
        
        FileSource<String> source = FileSource.forRecordStreamFormat(
        		new TextLineInputFormat(), 
        		new Path(input)
        		).build();
        
        DataStream<String> data = env.fromSource(
        		source, 
        		WatermarkStrategy.noWatermarks(), 
        		"File Source");
        
        // 2. CSV -> Tuple4
        
        DataStream<Tuple4<String, String, String, Integer>> mapped = data
        		.map(new Splitter())
        		.name("CSV Splitter");
        
        // 3. KeySelector: month
        KeySelector<Tuple4<String, String, String, Integer>, String> monthKey = value -> value.f0;
        
        // 4. SUM
        DataStream<Tuple4<String, String, String, Integer>> sumResult =
                mapped.keyBy(monthKey).sum(3);

        // 5. MIN
        DataStream<Tuple4<String, String, String, Integer>> minResult =
                mapped.keyBy(monthKey).min(3);

        // 6. MINBY
        DataStream<Tuple4<String, String, String, Integer>> minByResult =
                mapped.keyBy(monthKey).minBy(3);

        // 7. MAX
        DataStream<Tuple4<String, String, String, Integer>> maxResult =
                mapped.keyBy(monthKey).max(3);

        // 8. MAXBY
        DataStream<Tuple4<String, String, String, Integer>> maxByResult =
                mapped.keyBy(monthKey).maxBy(3);      
        
        // 9. FileSink
        // 9. FileSink
        writeToFile(sumResult, outputBase + "out1");
        writeToFile(minResult, outputBase + "out2");
        writeToFile(minByResult, outputBase + "out3");
        writeToFile(maxResult, outputBase + "out4");
        writeToFile(maxByResult, outputBase + "out5");
        

        // 10. Execute
        env.execute("Aggregation - Flink 2.2.1");
	}
	
	// FileSink 공통 함수
	private static void writeToFile(
            DataStream<Tuple4<String, String, String, Integer>> stream,
            String outputPath) {
		
		FileSink<String> sink = FileSink.forRowFormat(
				new Path(outputPath),
				new SimpleStringEncoder<String>("UTF-8")
				).build();
		
		stream.map(value->value.toString())
			.returns(String.class)
			.sinkTo(sink);
	}
	
	public static class Splitter implements
    MapFunction<String,
        Tuple4<String, String, String, Integer>> {
		
		@Override
		public Tuple4<String, String, String, Integer> map(String value) {
			String[] words = value.split(",");
			
            if (words.length != 5) {
                throw new IllegalArgumentException(
                        "Invalid CSV record: " + value);
            }
            
            return new Tuple4<>(
                    words[1].trim(),  // month
                    words[2].trim(),  // category
                    words[3].trim(),  // product
                    Integer.parseInt(words[4].trim()) // profit
            );

		}
	}
}
