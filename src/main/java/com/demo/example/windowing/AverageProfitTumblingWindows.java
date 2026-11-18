package com.demo.example.windowing;

import java.time.Duration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.ParameterTool;

public class AverageProfitTumblingWindows {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
        // STEP 1. 실행 파라미터
        ParameterTool params = ParameterTool.fromArgs(args);
        
        String host = params.get("host", "localhost");
        int port = params.getInt("port", 9090);
        
        String output = params.get("output");
        
        // STEP 2. Flink 실행 환경
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        
        // FileSink 파일 확정을 위한 Checkpoint
        env.enableCheckpointing(5000);
        
        // STEP 3. Socket Source
        DataStream<String> data =
                env.socketTextStream(host, port);
        
        // STEP 4. CSV -> Tuple5
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data
        		.map(new Splitter());
        
        // STEP 5. 월별 2초 Tumbling Window
        DataStream<Tuple5<String, String, String, Integer, Integer>>
            reduced = mapped.keyBy(value -> value.f0)
            		.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(2)))
            		.reduce(new Reduce1());
        
        // STEP 6. 결과 포맷
        DataStream<String> result = reduced.map(
        		new MapFunction<
        		 Tuple5<String, String, String, Integer, Integer>,
        		 String>() {

					@Override
					public String map(Tuple5<String, String, String, Integer, Integer> value) throws Exception {
	                    double avg =
	                            (double) value.f3 / value.f4;
	                    
	                    return String.format(
	                            "month=%s, category=%s, product=%s, " +
	                            "totalProfit=%d, count=%d, avgProfit=%.2f",
	                            value.f0,
	                            value.f1,
	                            value.f2,
	                            value.f3,
	                            value.f4,
	                            avg
	                        );
					}
        		});
        
        
        // STEP 7. Console 출력
        result.print();

        // STEP 8. FileSink
        FileSink<String> sink = FileSink
            .forRowFormat(
                new Path(output),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .build();

        result.sinkTo(sink);

        // STEP 9. Job 실행
        env.execute("Tumbling Window - Average Profit");

	}

    // CSV 문자열을 Tuple5로 변환
    public static class Splitter implements
        MapFunction<
            String,
            Tuple5<String, String, String, Integer, Integer>
        > {

        @Override
        public Tuple5<String, String, String, Integer, Integer>
            map(String value) {

            // 입력:
            // 01-06-2018,June,Category5,Bat,12

            String[] words = value.split(",");

            if (words.length != 5) {
                throw new IllegalArgumentException(
                    "Invalid CSV record: " + value
                );
            }

            return new Tuple5<>(
                words[1].trim(),                 // month
                words[2].trim(),                 // category
                words[3].trim(),                 // product
                Integer.parseInt(words[4].trim()), // profit
                1                                // count
            );
        }
    }

    // 동일한 월 + Window 내 Profit 누적
    public static class Reduce1 implements
        ReduceFunction<
            Tuple5<String, String, String, Integer, Integer>
        > {

        @Override
        public Tuple5<String, String, String, Integer, Integer>
            reduce(
                Tuple5<String, String, String, Integer, Integer> a,
                Tuple5<String, String, String, Integer, Integer> b
            ) {

            return new Tuple5<>(
                a.f0,              // month
                a.f1,              // category
                a.f2,              // product
                a.f3 + b.f3,       // totalProfit
                a.f4 + b.f4        // count
            );
        }
    }
}
