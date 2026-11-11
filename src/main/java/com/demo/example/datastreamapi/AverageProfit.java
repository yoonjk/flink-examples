package com.demo.example.datastreamapi;

import java.util.Spliterator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
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
        		new TextLineInputFormat(), 
        		new Path(input)
        	).build();
        
        DataStream<String> data = env.fromSource(
        		fileSource, 
        		WatermarkStrategy.noWatermarks(), 
        		"file Source");
        

        // 4. MapFunction
        // month, category, product, profit, count
        		
		// month, product, category, profit, count
		DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter()); // tuple
							
		// 5. KeyBy + ReduceFunction
		// [June,Category5,Bat,12,1]
		// [June,Category4,Perfume,10,1]

		// groupBy 'month'
		DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(t -> t.f0)
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
			reduced
				.map(new AverageMapper())
				.name("Average Profit Mapper");
		profitMonth.print();
	
		// 7. Convert to CSV string

		// execute program
		env.execute("Avg Profit Per Month");
		
		
	}

	// *************************************************************************
	// USER FUNCTIONS // pre_result = Category4,Perfume,22,2
	// *************************************************************************

	public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
		public Tuple5<String, String, String, Integer, Integer> reduce(
				Tuple5<String, String, String, Integer, Integer> current,
				Tuple5<String, String, String, Integer, Integer> pre_result) {
			return new Tuple5<String, String, String, Integer, Integer>(current.f0, current.f1, current.f2,
					current.f3 + pre_result.f3, current.f4 + pre_result.f4);
		}
	}

	public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
		public Tuple5<String, String, String, Integer, Integer> map(String value) // 01-06-2018,June,Category5,Bat,12
		{
			String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			// ignore timestamp, we don't need it for any calculations
			return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2], words[3],
					Integer.parseInt(words[4]), 1);
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
