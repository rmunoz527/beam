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

package org.apache.beam.sdk.io.parquet;


import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;


import org.apache.beam.sdk.io.Read;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;

import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BlockBasedSource;
import org.apache.hadoop.fs.Path;


import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import sun.net.www.content.text.Generic;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * IO utility to read and write Parquet files.
 *
 *
 *
 */

public class ParquetIO {


    public static class Read{

        public static Bound<GenericRecord> from(String path)
        {
            return new Bound<>(GenericRecord.class).from(path);
        }


        public static class Bound<T> extends PTransform<PBegin, PCollection<T>> {
            /**
             * The file path to read from.
             */
            @Nullable
            final String path;
            /**
             * The class type of the records.
             */
            final Class<T> type;
            /**
             * The schema of the input file.
             */
            //@Nullable
            //final Schema schema;
            /**
             * An option to indicate if input validation is desired. Default is true.
             */
            //final boolean validate;

            Bound(Class<T> type) {
                this(null, null, type);
            }

            Bound(String name, String path, Class<T> type) {
                super(name);
                this.path = path;
                this.type = type;
            }

            /**
             * Returns a new {@link PTransform} that's like this one but
             * that reads from the file(s) with the given name or pattern.
             *
             * <p>Does not modify this object.
             */
            public Bound<T> from(String path) {
                return new Bound<T>(name, path, type);
            }

            @Override
            public PCollection<T> expand(PBegin input) {

                if(path == null)
                    throw new IllegalStateException("Need to set the path of an ParquetIO.Read transform");


                @SuppressWarnings("unchecked")
                org.apache.beam.sdk.io.Read.Bounded<T> read = (org.apache.beam.sdk.io.Read.Bounded<T>) org.apache.beam.sdk.io.Read.from(
                        ParquetSource.from(new Path(path.toString())));

                       /*if(type == GenericRecord.class)
                               read = (org.apache.beam.sdk.io.Read.Bounded<T>) org.apache.beam.sdk.io.Read.from(
                                ParquetSource.from(path).withSchema(schema));
                        else
                          read =  org.apache.beam.sdk.io.Read.from(
                                ParquetSource.from(path).withSchema(type));*/

                PCollection<T> pcol = input.getPipeline().apply("Read", read);
                return pcol;
            }
        }
    }

    public static class Write{}



    // Disable instantiation of utility class
    private ParquetIO(){}
}
