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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

/** Provides classes to save/load model data. */
public class KMeansModelData {
    /** Converts the provided modelData Datastream into corresponding Table. */
    public static Table getModelDataTable(DataStream<DenseVector[]> modelData) {
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(modelData.getExecutionEnvironment());
        return tEnv.fromDataStream(modelData);
    }

    /** Converts the provided modelData Table into corresponding Datastream. */
    public static DataStream<DenseVector[]> getModelDataStream(Table table) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) table).getTableEnvironment();
        return tEnv.toDataStream(table).map(row -> (DenseVector[]) row.getField("f0"));
    }

    /** Encoder for the KMeans model data. */
    public static class ModelDataEncoder implements Encoder<DenseVector[]> {
        @Override
        public void encode(DenseVector[] modelData, OutputStream outputStream) throws IOException {
            IntSerializer intSerializer = new IntSerializer();
            DenseVectorSerializer denseVectorSerializer = new DenseVectorSerializer();
            DataOutputViewStreamWrapper outputViewStreamWrapper =
                    new DataOutputViewStreamWrapper(outputStream);
            intSerializer.serialize(modelData.length, outputViewStreamWrapper);
            for (DenseVector denseVector : modelData) {
                denseVectorSerializer.serialize(
                        denseVector, new DataOutputViewStreamWrapper(outputStream));
            }
        }
    }

    /** Decoder for the KMeans model data. */
    public static class ModelDataStreamFormat extends SimpleStreamFormat<DenseVector[]> {
        @Override
        public Reader<DenseVector[]> createReader(
                Configuration config, FSDataInputStream inputStream) {
            return new Reader<DenseVector[]>() {
                @Override
                public DenseVector[] read() throws IOException {
                    try {
                        IntSerializer intSerializer = new IntSerializer();
                        DenseVectorSerializer denseVectorSerializer = new DenseVectorSerializer();
                        DataInputViewStreamWrapper inputViewStreamWrapper =
                                new DataInputViewStreamWrapper(inputStream);
                        int numDenseVectors = intSerializer.deserialize(inputViewStreamWrapper);
                        DenseVector[] result = new DenseVector[numDenseVectors];
                        for (int i = 0; i < numDenseVectors; i++) {
                            result[i] = denseVectorSerializer.deserialize(inputViewStreamWrapper);
                        }
                        return result;
                    } catch (EOFException e) {
                        return null;
                    }
                }

                @Override
                public void close() throws IOException {
                    inputStream.close();
                }
            };
        }

        @Override
        public TypeInformation<DenseVector[]> getProducedType() {
            return ObjectArrayTypeInfo.getInfoFor(TypeInformation.of(DenseVector.class));
        }
    }
}
