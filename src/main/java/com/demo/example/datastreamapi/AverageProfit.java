package com.demo.example.datastreamapi;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

public class AverageProfit {
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
        FileSource fileSource = FileSource.forRecordStreamFormat(
        		new TextLineInputFormat(), // 파일을 한 줄씩 문자열로 읽음
        		new Path(input)
        	).build();
        
        DataStream<String> data = env.fromSource(
        		fileSource, 						// Source를 DataStream에 연결
        		WatermarkStrategy.noWatermarks(), 	// Event Time Watermark를 생성하지 않음
        		"file Source");
        
        // 4. Parse CSV
        // 4. MapFunction
        // month, category, product, profit, count
        		
		// month, product, category, profit, count
		DataStream<Tuple5<String, String, String, Long, Long>> mapped = data.map(new Splitter()); // CSV를 Tuple5로 변환
							
		// 5. KeyBy + ReduceFunction
		// [June,Category5,Bat,12,1]
		// [June,Category4,Perfume,10,1]

		// 5. Group by month and reduce
		// groupBy 'month'
		DataStream<Tuple5<String, String, String, Long, Long>> reduced = mapped.keyBy(t -> t.f0)
				.reduce(new Reduce1());
		
		// 6. Calculate average profit
		// June { [Category5,Bat,12,1] Category4,Perfume,10,1} //rolling reduce
		// reduced = { [Category4,Perfume,22,2] ..... }
		// month, avg. profit
//		DataStream<Tuple2<String, Double>> profitPerMonth = reduced
//				.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
//					public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) {
//						return new Tuple2<String, Double>(input.f0, new Double((input.f3 * 1.0) / input.f4));
//					}
//				});
		DataStream<Tuple2<String, Double>> profitMonth = 
				reduced.map(
						new MapFunction<Tuple5<String, String, String, Long, Long>,
	                    Tuple2<String, Double>>() {

							@Override
							public Tuple2<String, Double> map(Tuple5<String, String, String, Long, Long> input) {
								// TODO Auto-generated method stub
								// 계산식:
								
							
								//AverageProfit = \frac{TotalProfit}/{TotalCount}
								
								double average =
										(double) input.f3 / input.f4;
								
								return Tuple2.of(input.f0, average);
							}
					}).name("Calculate Average Profit");
                	;
	
		// 7. Convert to CSV string
        // 7. FileSink
        FileSink<Tuple2<String, Double>> sink = FileSink
        		.forRowFormat(
        				new Path(output),
        				new SimpleStringEncoder<Tuple2<String, Double>>("UTF-8")
        		).build();
        
        
        profitMonth.sinkTo(sink);
        
		// execute program
		env.execute("Avg Profit Per Month");
		
		
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

	public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Long, Long>> {
		public Tuple5<String, String, String, Long, Long> map(String value) // 01-06-2018,June,Category5,Bat,12
		{
			String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			
            if (words.length != 5) {
                throw new IllegalArgumentException(
                    "Invalid CSV record: " + value);
            }

            String month = words[1].trim();
            String category = words[2].trim();
            String product = words[3].trim();
            long profit = Long.parseLong(words[4].trim());
            
			// ignore timestamp, we don't need it for any calculations
            return Tuple5.of(
                    month,
                    category,
                    product,
                    profit,
                    1L
                );
		} // June Category5 Bat 12
	}

    // ----------------------------------
    // MapFunction: Calculate Average
    // ----------------------------------
    public static class AverageMapper
            implements MapFunction<
                Tuple5<String, String, String, Integer, Integer>,
                Tuple2<String, Double>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Double> map(
                Tuple5<String, String, String, Integer, Integer> input) {

            double average =
                (input.f3 * 1.0) / input.f4;

            return Tuple2.of(input.f0, average);
        }
    }

    // ----------------------------------
    // MapFunction: Tuple2 -> CSV
    // ----------------------------------
    public static class ResultMapper
            implements MapFunction<Tuple2<String, Double>, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String map(Tuple2<String, Double> value) {
            return value.f0 + "," + value.f1;
        }
    }
}
