package com.demo.example.join;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

public class JoinExample {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String input1 = params.getRequired("input1");
        String input2 = params.getRequired("input2");

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        // Person file source
        FileSource<String> personSource =
            FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(input1)
            ).build();

        // Location file source
        FileSource<String> locationSource =
            FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(input2)
            ).build();

        DataStream<Tuple2<Integer, String>> persons =
            env.fromSource(
                personSource,
                WatermarkStrategy.noWatermarks(),
                "Person Source"
            ).map(new CsvMapper());

        DataStream<Tuple2<Integer, String>> locations =
            env.fromSource(
                locationSource,
                WatermarkStrategy.noWatermarks(),
                "Location Source"
            ).map(new CsvMapper());

        // INNER JOIN by person_id
        DataStream<Tuple3<Integer, String, String>> joined =
            persons.keyBy(p -> p.f0)
                .connect(locations.keyBy(l -> l.f0))
                .process(new PersonLocationJoin());
     	
        // Tuple3 -> CSV
        DataStream<String> csvOutput = joined.map(
            row -> row.f0 + " " + row.f1 + " " + row.f2
        );

        // CSV File Sink
        OutputFileConfig fileConfig =
            OutputFileConfig.builder()
                .withPartPrefix("join-result")
                .withPartSuffix(".csv")
                .build();

        String output = params.getRequired("output");
        FileSink<String> fileSink =
            FileSink.forRowFormat(
                    new Path(output),
                    new SimpleStringEncoder<String>("UTF-8")
                )
                .withBucketAssigner(
                    new BasePathBucketAssigner<String>()
                )
                .withOutputFileConfig(fileConfig)
                .build();

        csvOutput.sinkTo(fileSink).name("CSV File Sink");

        // 디버깅용
        csvOutput.print("JOINED");

        env.execute("Join Example");
    }

    public static class CsvMapper
            implements MapFunction<String, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> map(String value) {
            String[] fields = value.split(",", 2);

            return Tuple2.of(
                Integer.parseInt(fields[0].trim()),
                fields[1].trim()
            );
        }
    }

    public static class PersonLocationJoin extends
            KeyedCoProcessFunction<
                Integer,
                Tuple2<Integer, String>,
                Tuple2<Integer, String>,
                Tuple3<Integer, String, String>> {

        private transient ListState<String> personState;
        private transient ListState<String> locationState;

        @Override
        public void open(
                org.apache.flink.api.common.functions.OpenContext context)
                throws Exception {

            personState = getRuntimeContext().getListState(
                new ListStateDescriptor<>(
                    "persons",
                    Types.STRING
                )
            );

            locationState = getRuntimeContext().getListState(
                new ListStateDescriptor<>(
                    "locations",
                    Types.STRING
                )
            );
        }

        @Override
        public void processElement1(
                Tuple2<Integer, String> person,
                Context ctx,
                Collector<Tuple3<Integer, String, String>> out)
                throws Exception {

            for (String location : locationState.get()) {
                out.collect(
                    Tuple3.of(person.f0, person.f1, location)
                );
            }

            personState.add(person.f1);
        }

        @Override
        public void processElement2(
                Tuple2<Integer, String> location,
                Context ctx,
                Collector<Tuple3<Integer, String, String>> out)
                throws Exception {

            for (String person : personState.get()) {
                out.collect(
                    Tuple3.of(location.f0, person, location.f1)
                );
            }

            locationState.add(location.f1);
        }
    }
}
