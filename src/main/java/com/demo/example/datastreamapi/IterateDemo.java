package com.demo.example.datastreamapi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

public class IterateDemo {
	public static void main(String[] args) throws Exception{
		
		// 1.Environment
		
		// 1. Input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		String output = params.get("output");

		// 2. Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		
        // Step 2. 입력 데이터 생성: 0 ~ 5
        DataStream<Long> numbers =
                env.fromData(0L, 1L, 2L, 3L, 4L, 5L);
        
        // Step 3. 초기 Tuple 생성
        DataStream<Tuple2<Long, Integer>> data =
                numbers.map(value ->
                        Tuple2.of(value, 0)
                ).returns(
                        new TypeHint<Tuple2<Long, Integer>>() {}
                    );
        
        // Step 4. 반복 계산
		// 4. raw Data
        // Step 4. 반복 계산
        DataStream<Tuple2<Long, Integer>> result =
                data.flatMap(
                    new FlatMapFunction<
                        Tuple2<Long, Integer>,
                        Tuple2<Long, Integer>>() {

                        @Override
                        public void flatMap(
                                Tuple2<Long, Integer> input,
                                Collector<Tuple2<Long, Integer>> out) {

                            long currentValue = input.f0;
                            int iterationCount = input.f1;

                            // 10이 될 때까지 반복
                            while (currentValue < 10) {

                                currentValue++;
                                iterationCount++;
                            }

                            // 최종 결과만 출력
                            if (currentValue == 10) {
                                out.collect(
                                    Tuple2.of(
                                        currentValue,
                                        iterationCount
                                    )
                                );
                            }
                        }
                    }
                ).returns(new TypeHint<Tuple2<Long, Integer>>() {});
        
        // 6. FileSink 생성
        FileSink<Tuple2<Long, Integer>> sink =
                FileSink.forRowFormat(
                    new Path(output),
                    new SimpleStringEncoder<Tuple2<Long, Integer>>("UTF-8")
                ).build();

        // 7. 결과를 파일로 저장
        result.sinkTo(sink);

        // Step 6. Flink Job 실행
        env.execute("Flink 2.2.1 Iterate Demo");
	}
}
