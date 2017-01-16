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

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BlockBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Created by rmunoz on 1/15/17.
 */

public class ParquetSource<T> extends BlockBasedSource<T>
{
    private ParquetIO.Read spec;
    private ParquetSource(ParquetIO.Read spec)
    {
        super("foo",200);
        this.spec = spec;
    }

    public ParquetSource(String fileOrPatternSpec, long minBundleSize) {
        super(fileOrPatternSpec, minBundleSize);
    }

    public ParquetSource(String fileName, long minBundleSize, long startOffset, long endOffset) {
        super(fileName, minBundleSize, startOffset, endOffset);
    }

    public static ParquetSource<GenericRecord> from(Path path)
    {
        return null;
    }


    @Override
    protected BlockBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end) {
        return null;
    }

    @Override
    protected BlockBasedReader<T> createSingleFileReader(PipelineOptions options) {
        return null;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
        return null;
    }

    class BlockParqueReader extends BlockBasedReader<T>{

        public BlockParqueReader(AvroSource<T> source)
        {
            super(source);
        }

        @Override
        public boolean readNextBlock() throws IOException {
            return false;
        }

        @Nullable
        @Override
        public Block<T> getCurrentBlock() {
            return null;
        }

        @Override
        public long getCurrentBlockSize() {
            return 0;
        }

        @Override
        public long getCurrentBlockOffset() {
            return 0;
        }

        @Override
        protected void startReading(ReadableByteChannel channel) throws IOException {

        }
    }
}
