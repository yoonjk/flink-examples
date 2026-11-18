package com.demo.example.watermark;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.util.ParameterTool;

public class WindowType {

	public static void main(String[] args) throws Exception{
        // STEP 1. 실행 파라미터
        ParameterTool params =
            ParameterTool.fromArgs(args);

        String output = params.get("output");
        String host = "localhost";
        int port = 9090;
        
        // STEP 2. Flink 실행 환경
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // STEP 3. Checkpoint 활성화
        env.enableCheckpointing(5000);
        
        // STEP 4. Data
        DataStream<String> data = env.socketTextStream(host, port);
        
        // STEP 5. CSV -> Tuple2<Long, Long>
        DataStream<Tuple2<Long,Long>> mapped = data.map(new EventMapper());
        
        // STEP 6. Timestamp + Watermark 지정
        WatermarkStrategy<Tuple2<Long, Long>> watermarkStrategy =
            WatermarkStrategy
                .<Tuple2<Long, Long>>forBoundedOutOfOrderness(
                    Duration.ofMillis(3500)
                )
                .withTimestampAssigner(
                    (event, previousTimestamp) -> event.f0
                );

        DataStream<Tuple2<Long, Long>> withWatermarks =
            mapped.assignTimestampsAndWatermarks(
                watermarkStrategy
            );
        
        // STEP 7. Event Time Session Window
        DataStream<Tuple2<Long, Long>> reduced =
            withWatermarks
                .windowAll(
                    EventTimeSessionWindows.withGap(
                        Duration.ofSeconds(10)
                    )
                )
                .reduce(new SumReducer());

        // STEP 8. 결과 문자열 변환
        DataStream<String> result = reduced.map(
            new MapFunction<Tuple2<Long, Long>, String>() {

                @Override
                public String map(Tuple2<Long, Long> value) {

                    return String.format(
                        "maxEventTime=%s, totalSum=%d",
                        Instant.ofEpochMilli(value.f0),
                        value.f1
                    );
                }
            }
        );

        // STEP 9. 콘솔 출력
        result.print();

        // STEP 10. FileSink
        FileSink<String> sink = FileSink
            .forRowFormat(
                new Path(output),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .build();

        result.sinkTo(sink);

        // STEP 11. Job 실행
        env.execute("Event Time Session Window");        
	}
    // CSV -> Tuple2
    public static class EventMapper implements
        MapFunction<String, Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> map(String value) {

            // 입력 예:
            // 1700000000000,10

            String[] words = value.split(",");

            if (words.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid CSV record: " + value
                );
            }

            long timestamp =
                Long.parseLong(words[0].trim());

            long amount =
                Long.parseLong(words[1].trim());

            return Tuple2.of(timestamp, amount);
        }
    }	
	
    public static class SumReducer implements ReduceFunction<Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> reduce(
        Tuple2<Long, Long> curr,
        Tuple2<Long, Long> prev
    ) {

        // 원본 이벤트의 최대 Timestamp 유지
        long maxTimestamp =
            Math.max(curr.f0, prev.f0);

        // 금액 합산
        long total =
            curr.f1 + prev.f1;

        return Tuple2.of(maxTimestamp, total);
    }
}
}
