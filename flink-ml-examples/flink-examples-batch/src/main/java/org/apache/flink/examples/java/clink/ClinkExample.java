package org.apache.flink.examples.java.clink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder;
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoderModel;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;

<<<<<<< HEAD
=======
import java.io.File;
>>>>>>> 1fd7a3ed683c8fde40b9e040c601a0a513f32034
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

public class ClinkExample {
    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tEnv;
    private static Schema schema;
    private static Row[] trainData;
    private static Row[] predictData;
    private static Row[] expectedClinkOutput;
    private static String[] inputCols;
    private static String[] outputCols;
    private static String clinkSoPath;
    private static String confRemotePath;
    private static String confLocalPath;

    private static OneHotEncoderModel getModel() {
        Table trainTable =
                tEnv.fromDataStream(
                        env.fromElements(trainData)
                                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks()),
                        schema);

        OneHotEncoder estimator =
                new OneHotEncoder().setInputCols(inputCols).setOutputCols(outputCols);

        OneHotEncoderModel model = estimator.fit(trainTable);

        return model;
    }

    private static Map<Object, Integer> getFrequencyMap(Row[] rows) {
        Map<Object, Integer> map = new HashMap<>();
        for (Row row : rows) {
            List<Object> list = getFieldValues(row);
            map.put(list, map.getOrDefault(list, 0) + 1);
        }
        return map;
    }

    private static List<Object> getFieldValues(Row row) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            list.add(row.getField(i));
        }
        return list;
    }

    private static void checkClinkResult(OneHotEncoderModel model) throws IOException {
        Table predictTable =
                tEnv.fromDataStream(
                        env.fromElements(predictData)
                                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks()),
                        schema);
        /** Temporarily hardcode some variables for clink transform test only */
        Table output =
                model.clinkTransform(clinkSoPath, confRemotePath, confLocalPath, predictTable)[0]
                        .select($("f0"));
        ;
        Object[] actualObjects = IteratorUtils.toArray(output.execute().collect());
        Row[] actual = new Row[actualObjects.length];
        for (int i = 0; i < actualObjects.length; i++) {
            actual[i] = (Row) actualObjects[i];
            System.out.println("Transformation result for line " + i + ": " + actual[i].toString());
        }

        Map<Object, Integer> expectedFreqMap = getFrequencyMap(expectedClinkOutput);
        Map<Object, Integer> actualFreqMap = getFrequencyMap(actual);
        assert expectedFreqMap.equals(actualFreqMap);
    }

    public static void main(String[] args) throws IOException {
<<<<<<< HEAD
        String rescDirPath = ClinkExample.class.getClassLoader().getResource("feature").getPath();
        System.out.println(rescDirPath);
        clinkSoPath =
                rescDirPath + "/libperception_feature_plugin.dylib";
        confRemotePath = "";
        confLocalPath = rescDirPath + "/clink_conf";
=======
        String rescDirPath = new File(".").getCanonicalPath();
        clinkSoPath =
                rescDirPath + "/src/resources/feature/libperception_feature_plugin.dylib";
        confRemotePath = "";
        confLocalPath = rescDirPath + "/src/resources/feature/clink_conf";
>>>>>>> 1fd7a3ed683c8fde40b9e040c601a0a513f32034
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("clinkSoPath")) {
            clinkSoPath = params.getRequired("clinkSoPath");
        }
        if (params.has("confRemotePath")) {
            confRemotePath = params.getRequired("confRemotePath");
        }
        if (params.has("confLocalPath")) {
            confLocalPath = params.getRequired("confLocalPath");
        }

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.DOUBLE())
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build();

        trainData =
                new Row[] {
<<<<<<< HEAD
                        Row.of(0.0), Row.of(1.0), Row.of(2.0), Row.of(0.0),
=======
                    Row.of(0.0), Row.of(1.0), Row.of(2.0), Row.of(0.0),
>>>>>>> 1fd7a3ed683c8fde40b9e040c601a0a513f32034
                };

        predictData = trainData;

        expectedClinkOutput =
                new Row[] {
<<<<<<< HEAD
                        Row.of(Row.of("0:0 ")),
                        Row.of(Row.of("0:1 ")),
                        Row.of(Row.of("0:0 ")),
                        Row.of(Row.of("0:2 ")),
=======
                    Row.of(Row.of("0:0 ")),
                    Row.of(Row.of("0:1 ")),
                    Row.of(Row.of("0:0 ")),
                    Row.of(Row.of("0:2 ")),
>>>>>>> 1fd7a3ed683c8fde40b9e040c601a0a513f32034
                };

        inputCols = new String[] {"f0"};
        outputCols = new String[] {"output_f0"};

        OneHotEncoderModel model = getModel();
        checkClinkResult(model);
    }
<<<<<<< HEAD
}
=======
}
>>>>>>> 1fd7a3ed683c8fde40b9e040c601a0a513f32034
