package com.demo.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.util.ParameterTool;

public class WordCount {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        // --input이 없으면 명확한 오류로 종료합니다.
        String inputPath = params.getRequired("input");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 유한한 파일 입력을 처리하므로 BATCH 모드를 사용합니다.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().setGlobalJobParameters(params);

        // monitorContinuously를 설정하지 않으므로 파일을 읽고 종료합니다.
        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(inputPath))
                        .build();

        DataStream<String> text =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Text File Source");

        DataStream<Tuple2<String, Integer>> counts =
                text
                        // 기존 조건 유지: 대문자 N으로 시작하는 줄만 선택
                        .filter(value -> value.startsWith("N"))

                        // 줄 전체를 (문자열, 1)로 변환
                        .map(new Tokenizer())

                        // 동일한 문자열을 기준으로 집계
                        .keyBy(value -> value.f0)
                        .sum(1);

        if (params.has("output")) {

            // 기존 출력 형식 유지: 문자열 + 공백 + 개수
            DataStream<String> output =
                    counts.map(value -> value.f0 + " " + value.f1);

            FileSink<String> sink =
                    FileSink
                            .forRowFormat(
                                    new Path(params.getRequired("output")),
                                    new SimpleStringEncoder<String>("UTF-8"))

                            // 시간별 하위 디렉터리 생성을 생략합니다.
                            .withBucketAssigner(
                                    new BasePathBucketAssigner<String>())
                            .build();

            output.sinkTo(sink).name("Count File Sink");

        } else {
            // 클러스터에서는 TaskManager 로그에서 확인합니다.
            counts.print();
        }

        // 출력 방식과 관계없이 작업을 실행합니다.
        env.execute("WordCount Example");
    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) {
            return Tuple2.of(value, 1);
        }
    }
}