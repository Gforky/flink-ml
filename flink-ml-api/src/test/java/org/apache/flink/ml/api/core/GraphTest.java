/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.core;

import org.apache.flink.ml.api.core.ExampleStages.SumEstimator;
import org.apache.flink.ml.api.core.ExampleStages.SumModel;
import org.apache.flink.ml.api.core.ExampleStages.UnionAlgoOperator;
import org.apache.flink.ml.api.graph.Graph;
import org.apache.flink.ml.api.graph.GraphBuilder;
import org.apache.flink.ml.api.graph.TableId;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Tests the behavior of {@link Graph}. */
public class GraphTest extends AbstractTestBase {
    @Test
    public void testTransformerChain() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        GraphBuilder builder = new GraphBuilder();
        int delta = 1;

        // Creates nodes
        SumModel stage1 = new SumModel();
        stage1.setModelData(tEnv.fromValues(delta));
        SumModel stage2 = new SumModel();
        stage2.setModelData(tEnv.fromValues(delta));
        SumModel stage3 = new SumModel();
        stage3.setModelData(tEnv.fromValues(delta));
        // Creates inputs and inputStates
        TableId input1 = builder.createTableId();
        // Feeds inputs to nodes and gets outputs.
        TableId output1 = builder.addAlgoOperator(stage1, input1)[0];
        TableId output2 = builder.addAlgoOperator(stage2, output1)[0];
        TableId output3 = builder.addAlgoOperator(stage3, output2)[0];

        TableId[] inputs = new TableId[] {input1};
        TableId[] outputs = new TableId[] {output3};
        Model<?> model = builder.buildModel(inputs, outputs);

        Table inputTable = tEnv.fromDataStream(env.fromCollection(Arrays.asList(1, 2, 3)));
        Table outputTable = model.transform(inputTable)[0];

        List<Integer> output =
                IteratorUtils.toList(
                        tEnv.toDataStream(outputTable, Integer.class).executeAndCollect());
        List<Integer> expectedOutput = Arrays.asList(4, 5, 6);
        compareResultCollections(expectedOutput, output, Comparator.naturalOrder());
    }

    @Test
    public void testTransformerDAG() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        GraphBuilder builder = new GraphBuilder();
        int delta = 1;
        // Creates nodes
        SumModel stage1 = new SumModel();
        stage1.setModelData(tEnv.fromValues(delta));
        SumModel stage2 = new SumModel();
        stage2.setModelData(tEnv.fromValues(delta));
        AlgoOperator<?> stage3 = new UnionAlgoOperator();

        // Creates inputs and inputStates
        TableId input1 = builder.createTableId();
        TableId input2 = builder.createTableId();
        // Feeds inputs to nodes and gets outputs.
        TableId output1 = builder.addAlgoOperator(stage1, input1)[0];
        TableId output2 = builder.addAlgoOperator(stage2, input2)[0];
        TableId output3 = builder.addAlgoOperator(stage3, output1, output2)[0];

        TableId[] inputs = new TableId[] {input1, input2};
        TableId[] outputs = new TableId[] {output3};

        Model<?> model = builder.buildModel(inputs, outputs);

        Table inputA = tEnv.fromDataStream(env.fromCollection(Arrays.asList(1, 2, 3)));
        Table inputB = tEnv.fromDataStream(env.fromCollection(Arrays.asList(10, 11, 12)));
        Table outputTable = model.transform(inputA, inputB)[0];

        List<Integer> output =
                IteratorUtils.toList(
                        tEnv.toDataStream(outputTable, Integer.class).executeAndCollect());

        List<Integer> expectedOutput = Arrays.asList(2, 3, 4, 11, 12, 13);
        compareResultCollections(expectedOutput, output, Comparator.naturalOrder());
    }

    @Test
    public void testEstimatorDAGWithGraphModel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        GraphBuilder builder = new GraphBuilder();
        // Creates nodes
        Estimator<?, ?> stage1 = new SumEstimator();
        Estimator<?, ?> stage2 = new SumEstimator();
        AlgoOperator<?> stage3 = new UnionAlgoOperator();
        // Creates inputs and inputStates
        TableId input1 = builder.createTableId();
        TableId input2 = builder.createTableId();
        // Feeds inputs to nodes and gets outputs.
        TableId output1 = builder.addEstimator(stage1, input1)[0];
        TableId output2 = builder.addEstimator(stage2, input2)[0];
        TableId output3 = builder.addAlgoOperator(stage3, output1, output2)[0];

        TableId[] inputs = new TableId[] {input1, input2};
        TableId[] outputs = new TableId[] {output3};

        Model<?> model = builder.buildModel(inputs, outputs);

        Table inputA = tEnv.fromDataStream(env.fromCollection(Arrays.asList(1, 2, 3)));
        Table inputB = tEnv.fromDataStream(env.fromCollection(Arrays.asList(10, 11, 12)));
        Table outputTable = model.transform(inputA, inputB)[0];

        List<Integer> output =
                IteratorUtils.toList(
                        tEnv.toDataStream(outputTable, Integer.class).executeAndCollect());

        List<Integer> expectedOutput = Arrays.asList(7, 8, 9, 43, 44, 45);
        compareResultCollections(expectedOutput, output, Comparator.naturalOrder());
    }

    @Test
    public void testEstimatorDAGWithGraph() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);

        GraphBuilder builder = new GraphBuilder();
        // Creates nodes
        Estimator<?, ?> stage1 = new SumEstimator();
        Estimator<?, ?> stage2 = new SumEstimator();
        AlgoOperator<?> stage3 = new UnionAlgoOperator();
        // Creates inputs and inputStates
        TableId input1 = builder.createTableId();
        TableId input2 = builder.createTableId();
        // Feeds inputs to nodes and gets outputs.
        TableId output1 = builder.addEstimator(stage1, input1)[0];
        TableId output2 = builder.addEstimator(stage2, input2)[0];
        TableId output3 = builder.addAlgoOperator(stage3, output1, output2)[0];

        TableId[] inputs = new TableId[] {input1, input2};
        TableId[] outputs = new TableId[] {output3};

        Estimator<?, ?> estimator = builder.buildEstimator(inputs, outputs);

        Table inputA = tEnv.fromDataStream(env.fromCollection(Arrays.asList(1, 2, 3)));
        Table inputB = tEnv.fromDataStream(env.fromCollection(Arrays.asList(10, 11, 12)));

        Model<?> model = estimator.fit(inputA, inputB);
        Table outputTable = model.transform(inputA, inputB)[0];

        List<Integer> output =
                IteratorUtils.toList(
                        tEnv.toDataStream(outputTable, Integer.class).executeAndCollect());

        List<Integer> expectedOutput = Arrays.asList(7, 8, 9, 43, 44, 45);
        compareResultCollections(expectedOutput, output, Comparator.naturalOrder());
    }
}
