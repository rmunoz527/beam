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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ParquetIOTest {

    Random random = new Random(0L);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void directWrite() throws Exception {


        Schema schema = Schema.createRecord("myrecord", null, null, false);

        schema.setFields(Arrays.asList(new Schema.Field[]{
                new Schema.Field("ID", Schema.create(Schema.Type.INT), null, 1)
        }));
        File file = temporaryFolder.newFile("default.parquet");
        file.delete();
        Path path = new Path(file.toString());


        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .build();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        GenericRecord record = builder.build();

        writer.write(record);

        writer.close();

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
        GenericRecord nextRecord = reader.read();
        reader.close();
        Assert.assertNotNull(nextRecord);
        Assert.assertEquals(nextRecord.get("ID"),1);
    }

    @Test
    public void testDecimalValues() throws Exception{
        Schema decimalSchema = Schema.createRecord("myrecord",null,null,false);
        Schema decimal = LogicalTypes.decimal(9,2)
                .addToSchema(Schema.create(Schema.Type.BYTES));
        decimalSchema.setFields(Collections.singletonList(new Schema.Field("dec", decimal,null,null)));

        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());

        File file = temporaryFolder.newFile("decimal.parquest");
        file.delete();
        Path path = new Path(file.toString());

        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withDataModel(decimalSupport)
                .withSchema(decimalSchema)
                .build();

        Random random = new Random(34L);
        GenericRecordBuilder builder = new GenericRecordBuilder(decimalSchema);
        List<GenericRecord> expected = Lists.newArrayList();
        for(int i = 0; i < 1000; i += 1)
        {
            BigDecimal dec = new BigDecimal(new BigInteger(31,random),2);
            builder.set("dec",dec);

            GenericRecord rec = builder.build();
            expected.add(rec);
            writer.write(builder.build());
        }
        writer.close();

        ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(path)
                .withDataModel(decimalSupport)
                .disableCompatibility()
                .build();
        List<GenericRecord> records = Lists.newArrayList();
        GenericRecord rec;
        while((rec = reader.read()) != null){
            records.add(rec);
        }
        reader.close();
        Assert.assertTrue("dec field should be a BigDecimal instance",
                records.get(0).get("dec") instanceof BigDecimal);
        Assert.assertEquals("Content should match", expected,records);
    }

    @Test
    public void testPipeline() throws Exception{
       // PCollection<GenericRecord> pCol = pipeline.apply(
       //         ParquetIO.Read()
       // );
    }

    private File createFileWithData(String filename, List<KV<IntWritable, Text>> records)
            throws IOException {
        File tmpFile = temporaryFolder.newFile(filename);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
                SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.file(new Path(tmpFile.toURI())))) {

            for (KV<IntWritable, Text> record : records) {
                writer.append(record.getKey(), record.getValue());
            }
        }
        return tmpFile;
    }

    private List<KV<IntWritable, Text>> createRandomRecords(int dataItemLength,
                                                            int numItems, int offset) {
        List<KV<IntWritable, Text>> records = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            IntWritable key = new IntWritable(i + offset);
            Text value = new Text(createRandomString(dataItemLength));
            records.add(KV.of(key, value));
        }
        return records;
    }

    private String createRandomString(int length) {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(chars[random.nextInt(chars.length)]);
        }
        return builder.toString();
    }


}