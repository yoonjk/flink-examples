package com.demo.example.datastreamapi;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.ParameterTool;

public class SplitDemo {
    // Side Output tags
    private static final OutputTag<Integer> ODD_TAG =
        new OutputTag<Integer>("odd-output") {};

    private static final OutputTag<String> ERROR_TAG =
        new OutputTag<String>("error-output") {};
	public static void main(String[] args) throws Exception {

		// 1. Input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        String input= params.get("input");
        String output= params.get("output");
        
        // 2. Execution environment
        final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);              

        // 3. FileSource
        FileSource source = FileSource.forRecordStreamFormat(
        		new TextLineInputFormat(), 
        		new Path(input)
        		).build();
        
        // data
        DataStream<String> data = env.fromSource(
        		source, 
        		WatermarkStrategy.noWatermarks(), 
        		"file Source"
        		);
        
        // 4. Split even and odd numbers
        SingleOutputStreamOperator<Integer> evenStream = data
        		.process(new EvenOddProcessFunction())
        		.name("Even Odd Split");
        		
        // 5. Side Output: odd numbers
        DataStream<Integer> oddStream =
            evenStream.getSideOutput(ODD_TAG);
        
        // 6. Side Output: invalid input
        DataStream<String> errorStream =
            evenStream.getSideOutput(ERROR_TAG);
        
        // 7. Even FileSink
        FileSink<Integer> sink = FileSink.forRowFormat(
        		new Path(output + "/even"), 
        		new SimpleStringEncoder<Integer>("UTF-8")
        		).build();
        
        evenStream.sinkTo(sink)
        .name("Even Number Sink");
        
        // 8. Odd FileSink
        FileSink<Integer> oddSink = FileSink.forRowFormat(
        		new Path(output + "/odd"), 
        		new SimpleStringEncoder<Integer>("UTF-8")
        		).build();
        
        oddStream.sinkTo(oddSink)
        	.name("Odd Number Sink");
        
        // 9. Error FileSink
        FileSink<String> errorSink = FileSink.forRowFormat(
        		new Path(output + "/errors"), 
        		new SimpleStringEncoder<String>("UTF-8")
        	).build();
        
        errorStream.sinkTo(errorSink)
        .name("Invalid Number Sink");
        
        // 10. Execute
        env.execute("Even Odd Split Demo");
	}
	
	// User-defined ProcessFunction
	public static class EvenOddProcessFunction extends ProcessFunction<String, Integer> {

		@Override
		public void processElement(String value, ProcessFunction<String, Integer>.Context ctx, Collector<Integer> out)
				throws Exception {
			try {
				int number = Integer.parseInt(value.trim());
				
				if (number % 2 == 0) {
                    // Main Output: even
                    out.collect(number);
				} else {

                    // Side Output: odd
                    ctx.output(ODD_TAG, number);
                }
			} catch (NumberFormatException ex) {
                // Side Output: invalid number
                ctx.output(
                    ERROR_TAG,
                    "INVALID_NUMBER," + value
                );
			}
			
		}
		
	}
}
