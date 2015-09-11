/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * Verify MemoryManager could adjust its writers' allocated memory size.
 */
public class TestMemoryManager {

  Configuration conf = new Configuration();
  String writeSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";
  long expectedPoolSize;
  ParquetOutputFormat parquetOutputFormat;
  int counter = 0;

  @Before
  public void setUp() throws Exception {
    parquetOutputFormat = new ParquetOutputFormat(new GroupWriteSupport());

    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), conf);
    expectedPoolSize = Math.round((double)
        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() *
        MemoryManager.DEFAULT_MEMORY_POOL_RATIO);

    long rowGroupSize = expectedPoolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    // the memory manager is not initialized until a writer is created
    createWriter(1).close(null);
  }

  @Test
  public void testMemoryManagerUpperLimit() {
    // Verify the memory pool size
    // this value tends to change a little between setup and tests, so this
    // validates that it is within 5% of the expected value
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    Assert.assertTrue("Pool size should be within 5% of the expected value",
        Math.abs(expectedPoolSize - poolSize) < (long) (expectedPoolSize * 0.05));
  }

  @Test
  public void testMemoryManager() throws Exception {
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    long rowGroupSize = poolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    Assert.assertTrue("Pool should hold 2 full row groups",
        (2 * rowGroupSize) <= poolSize);
    Assert.assertTrue("Pool should not hold 3 full row groups",
        poolSize < (3 * rowGroupSize));

    Assert.assertEquals("Allocations should start out at 0",
        0, getTotalAllocation());

    RecordWriter writer1 = createWriter(1);
    Assert.assertTrue("Allocations should never exceed pool size",
        getTotalAllocation() <= poolSize);
    Assert.assertEquals("First writer should be limited by row group size",
        rowGroupSize, getTotalAllocation());

    RecordWriter writer2 = createWriter(2);
    Assert.assertTrue("Allocations should never exceed pool size",
        getTotalAllocation() <= poolSize);
    Assert.assertEquals("Second writer should be limited by row group size",
        2 * rowGroupSize, getTotalAllocation());

    RecordWriter writer3 = createWriter(3);
    Assert.assertTrue("Allocations should never exceed pool size",
        getTotalAllocation() <= poolSize);

    writer1.close(null);
    Assert.assertTrue("Allocations should never exceed pool size",
        getTotalAllocation() <= poolSize);
    Assert.assertEquals("Allocations should be increased to the row group size",
        2 * rowGroupSize, getTotalAllocation());

    writer2.close(null);
    Assert.assertTrue("Allocations should never exceed pool size",
        getTotalAllocation() <= poolSize);
    Assert.assertEquals("Allocations should be increased to the row group size",
        rowGroupSize, getTotalAllocation());

    writer3.close(null);
    Assert.assertEquals("Allocations should be increased to the row group size",
        0, getTotalAllocation());
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private RecordWriter createWriter(int index) throws Exception {
    File file = temp.newFile(String.valueOf(index) + ".parquet");
    file.delete();
    RecordWriter writer = parquetOutputFormat.getRecordWriter(
        conf, new Path(file.toString()),
        CompressionCodecName.UNCOMPRESSED);

    return writer;
  }

  private long getTotalAllocation() {
    Set<InternalParquetRecordWriter> writers = ParquetOutputFormat
        .getMemoryManager().getWriterList().keySet();
    long total = 0;
    for (InternalParquetRecordWriter writer : writers) {
      total += writer.getRowGroupSizeThreshold();
    }
    return total;
  }
}
