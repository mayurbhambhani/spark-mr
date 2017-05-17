/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package parquet;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;

public class ParquetRecordWriterWrapper extends RecordWriter<NullWritable, ParquetHiveRecord> {

  public static final Log LOG = LogFactory.getLog(ParquetRecordWriterWrapper.class);

  private final org.apache.hadoop.mapreduce.RecordWriter<Void, ParquetHiveRecord> realWriter;
  private final TaskAttemptContext taskContext;

  public ParquetRecordWriterWrapper(final OutputFormat<Void, ParquetHiveRecord> realOutputFormat,
      final Configuration jobConf, final String name) throws IOException {
    try {
      // create a TaskInputOutputContext
      TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }
      taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);

      LOG.info("initialize serde with table properties.");

      LOG.info("creating real writer to write at " + name);

      realWriter =
          ((ParquetOutputFormat) realOutputFormat).getRecordWriter(taskContext, new Path(name));

      LOG.info("real writer: " + realWriter);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }


  public void write(NullWritable key, ParquetHiveRecord value) throws IOException {
    try {
      realWriter.write(null, value);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }



  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      realWriter.close(taskContext);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }



}
