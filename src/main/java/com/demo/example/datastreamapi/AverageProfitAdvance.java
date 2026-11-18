package com.demo.example.datastreamapi;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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

public class AverageProfitAdvance {
    // Invalid records output
    private static final OutputTag<String> ERROR_TAG =
        new OutputTag<String>("invalid-records") {};
	
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("dd-MM-uuuu")
                .withResolverStyle(ResolverStyle.STRICT);
	
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // File-based batch processing
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // 2. Parameters
        final ParameterTool params =
            ParameterTool.fromArgs(args);
        
        String input = params.get("input");
        String output = params.get("output");
        
        // 3. FileSource
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
        		new TextLineInputFormat(), // 파일을 한 줄씩 문자열로 읽음
        		new Path(input)
        	).build();
        
        DataStream<String> rawData = env.fromSource(
        		fileSource, 						// Source를 DataStream에 연결
        		WatermarkStrategy.noWatermarks(), 	// Event Time Watermark를 생성하지 않음
        		"file Source");
        
        // 4. Parse CSV
        // 4. MapFunction
        // month, category, product, profit, count
        		
        // 4. Parse and validate CSV
        SingleOutputStreamOperator<
            Tuple3<String, Long, Long>> validData =
            rawData.process(new CsvParser())
                .name("Validate Profit CSV");
        
        // 5. Extract invalid records
        DataStream<String> invalidData =
            validData.getSideOutput(ERROR_TAG);        
		
        // 6. Aggregate by year-month
        DataStream<Tuple3<String, Long, Long>> aggregated =
            validData
                .keyBy(record -> record.f0)
                .reduce(new ProfitReducer())
                .name("Monthly Profit Aggregation");      
        
        // 7. Calculate monthly average
        DataStream<Tuple2<String, Double>> averageProfit =
            aggregated
                .map(new AverageCalculator())
                .name("Calculate Monthly Average");
        
        // 8. Format output as CSV
        DataStream<String> resultCsv = averageProfit.map(
        		(MapFunction<Tuple2<String, Double>, String>)
        			record -> record.f0 + "," + record.f1
        		).name("Format Result CSV");
        
        // 9. Result FileSink
        FileSink<String> resultSink = FileSink.forRowFormat(
        		new Path(output  + "/result"), 
        		new SimpleStringEncoder<String>("UTF8")
        		).build();


        resultCsv.sinkTo(resultSink)
        .name("Monthly Average Result Sink");
        
        // 10. Error FileSink
        FileSink<String> errorSink =
            FileSink.forRowFormat(
                new Path(output + "/errors"),
                new SimpleStringEncoder<String>("UTF-8")
            ).build();
		
		invalidData.sinkTo(errorSink);
		
        // 11. Execute
        env.execute("Average Profit - Production Style");
	}

	// *************************************************************************
	// USER FUNCTIONS // pre_result = Category4,Perfume,22,2
	// *************************************************************************

	public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Long, Long>> {
		public Tuple5<String, String, String, Long, Long> reduce(
				Tuple5<String, String, String, Long, Long> current,
				Tuple5<String, String, String, Long, Long> pre_result) {
			return new Tuple5<String, String, String, Long, Long>(current.f0, current.f1, current.f2,
					current.f3 + pre_result.f3, current.f4 + pre_result.f4);
		}
	}

	public static class CsvParser extends ProcessFunction<String, Tuple3<String, Long, Long>> {
	    // Invalid records output
	    private static final OutputTag<String> ERROR_TAG =
	        new OutputTag<String>("invalid-records") {};
		
        @Override
        public void processElement(
            String value,
            Context ctx,
            Collector<Tuple3<String, Long, Long>> out) {
        	
        	try {
        		List<String> fields = parseCsv(value);
        		
                if (fields.size() != 5) {
                    throw new IllegalArgumentException(
                        "Expected 5 fields");
                }
                
                LocalDate date = LocalDate.parse(
                        fields.get(0).trim(), DATE_FORMAT);
                
                String monthName = fields.get(1).trim();
                
                String expectedMonth =
                        date.getMonth().getDisplayName(
                            java.time.format.TextStyle.FULL,
                            Locale.ENGLISH);
                if (!expectedMonth.equalsIgnoreCase(monthName)) {
                    throw new IllegalArgumentException(
                        "Month does not match date");
                }
                
                String category = fields.get(2).trim();
                String product = fields.get(3).trim();
                
                if (category.isEmpty() || product.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Category or product is empty");
                }
                
                long profit =
                        Long.parseLong(fields.get(4).trim());

                String yearMonth =
                    YearMonth.from(date).toString();
                
                out.collect(Tuple3.of(
                        yearMonth,
                        profit,
                        1L
                    ));
        	} catch (IllegalArgumentException ex) {
                ctx.output(
                        ERROR_TAG,
                        "ERROR: " + ex.getMessage()
                            + " | DATA: " + value
                    );
        	}
        }
        
        private static List<String> parseCsv(String line) {

            List<String> fields = new ArrayList<>();
            StringBuilder field = new StringBuilder();
            boolean quoted = false;

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);

                if (c == '"') {
                    if (quoted && i + 1 < line.length()
                            && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = !quoted;
                    }
                } else if (c == ',' && !quoted) {
                    fields.add(field.toString());
                    field.setLength(0);
                } else {
                    field.append(c);
                }
            }

            if (quoted) {
                throw new IllegalArgumentException(
                    "Unclosed quoted field");
            }

            fields.add(field.toString());
            return fields;
        }        
	}
    
	 // Monthly sum and count
    // Monthly sum and count
    public static class ProfitReducer implements
        ReduceFunction<Tuple3<String, Long, Long>> {

        @Override
        public Tuple3<String, Long, Long> reduce(
            Tuple3<String, Long, Long> a,
            Tuple3<String, Long, Long> b) {

            return Tuple3.of(
                a.f0,
                Math.addExact(a.f1, b.f1),
                Math.addExact(a.f2, b.f2)
            );
        }
    }

    // Calculate average
    public static class AverageCalculator implements
        MapFunction<
            Tuple3<String, Long, Long>,
            Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(
            Tuple3<String, Long, Long> input) {

            double average =
                (double) input.f1 / input.f2;

            return Tuple2.of(input.f0, average);
        }
    }
}
