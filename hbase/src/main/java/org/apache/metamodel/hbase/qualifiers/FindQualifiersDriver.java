/**
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
package org.apache.metamodel.hbase.qualifiers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FindQualifiersDriver extends Configured implements Tool {

    static class OnlyColumnNameMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, final Context context) throws IOException,
                InterruptedException {
            CellScanner cellScanner = value.cellScanner();
            while (cellScanner.advance()) {

                Cell cell = cellScanner.current();
                byte[] q = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                context.write(new Text(q), new Text());
            }
        }
    }

    static class OnlyColumnNameReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            context.write(new Text(key), new Text());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path outputPath = new Path("output/");
        byte[] tableName = new String("ietsanders2").getBytes();
        byte[] columnFamilyName = new String("data").getBytes();

        Configuration configuration = createConfig();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(outputPath, true);

        Job job = Job.getInstance(configuration, "Distinct_columns");
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.setBatch(500);
        scan.addFamily(columnFamilyName);
        scan.setFilter(new KeyOnlyFilter()); // scan only key part of KeyValue (raw, column family, column)
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        TextOutputFormat.setOutputPath(job, outputPath);

        TableMapReduceUtil.initTableMapperJob(tableName, scan, OnlyColumnNameMapper.class, // mapper
                Text.class, // mapper output key
                Text.class, // mapper output value
                job);

        job.setNumReduceTasks(1);
        job.setReducerClass(OnlyColumnNameReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    protected Configuration createConfig() {
        Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "bigdatavm");
        config.set("hbase.zookeeper.property.clientPort", Integer.toString(2181));
        config.set("hbase.client.retries.number", Integer.toString(1));
        config.set("zookeeper.session.timeout", Integer.toString(5000));
        config.set("zookeeper.recovery.retry", Integer.toString(1));
        return config;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FindQualifiersDriver(), args);
        System.exit(exitCode);
    }
}
