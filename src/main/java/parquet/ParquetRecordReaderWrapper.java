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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Strings;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageTypeParser;

public class ParquetRecordReaderWrapper extends RecordReader<NullWritable, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ParquetRecordReaderWrapper.class);

  private long splitLen; // for getPos()

  private org.apache.hadoop.mapreduce.RecordReader<Void, ArrayWritable> realReader;
  // expect readReader return same Key & Value objects (common case)
  // this avoids extra serialization & deserialization of these objects
  private ArrayWritable valueObj = null;
  private int schemaSize;
  private boolean skipTimestampConversion = false;
  private Configuration jobConf;
  private List<BlockMetaData> filtedBlocks;
  private NullWritable key = NullWritable.get();


  private Class oldJobConf;

  private ParquetInputFormat<ArrayWritable> newInputFormat;

  public ParquetRecordReaderWrapper(ParquetInputFormat<ArrayWritable> newInputFormat) {
    this.newInputFormat = newInputFormat;
  }



  @Override
  public void close() throws IOException {
    if (realReader != null) {
      realReader.close();
    }
  }



  public long getPos() throws IOException {
    return (long) (splitLen * getProgress());
  }

  @Override
  public float getProgress() throws IOException {
    if (realReader == null) {
      return 1f;
    } else {
      try {
        return realReader.getProgress();
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }
  }



  /**
   * gets a ParquetInputSplit corresponding to a split given by Hive
   *
   * @param oldSplit The split given by Hive
   * @param conf The JobConf of the Hive job
   * @return a ParquetInputSplit corresponding to the oldSplit
   * @throws IOException if the config cannot be enhanced or if the footer cannot be read from the
   *         file
   */
  @SuppressWarnings("deprecation")
  protected ParquetInputSplit getSplit(final InputSplit oldSplit, final Configuration conf)
      throws IOException {
    ParquetInputSplit split;
    if (oldSplit instanceof FileSplit) {
      final Path finalPath = ((FileSplit) oldSplit).getPath();

      final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(jobConf, finalPath);
      final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
      final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

      final ReadContext readContext = new DataWritableReadSupport()
          .init(new InitContext(jobConf, null, fileMetaData.getSchema()));
      schemaSize = MessageTypeParser.parseMessageType(readContext.getReadSupportMetadata()
          .get(DataWritableReadSupport.HIVE_TABLE_AS_PARQUET_SCHEMA)).getFieldCount();
      final List<BlockMetaData> splitGroup = new ArrayList<BlockMetaData>();
      final long splitStart = ((FileSplit) oldSplit).getStart();
      final long splitLength = ((FileSplit) oldSplit).getLength();
      for (final BlockMetaData block : blocks) {
        final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
        if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
          splitGroup.add(block);
        }
      }
      if (splitGroup.isEmpty()) {
        LOG.warn("Skipping split, could not find row group in: " + (FileSplit) oldSplit);
        return null;
      }
      filtedBlocks = splitGroup;
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
        skipTimestampConversion =
            !Strings.nullToEmpty(fileMetaData.getCreatedBy()).startsWith("parquet-mr");
      }
      split = new ParquetInputSplit(finalPath, splitStart, splitLength,
          ((FileSplit) oldSplit).getLocations(), filtedBlocks,
          readContext.getRequestedSchema().toString(), fileMetaData.getSchema().toString(),
          fileMetaData.getKeyValueMetaData(), readContext.getReadSupportMetadata());
      return split;
    } else {
      throw new IllegalArgumentException("Unknown split type: " + oldSplit);
    }
  }

  public List<BlockMetaData> getFiltedBlocks() {
    return filtedBlocks;
  }



  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.splitLen = split.getLength();
    this.jobConf = context.getConfiguration();
    final ParquetInputSplit parquetSplit = getSplit(split, jobConf);

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get(IOConstants.MAPRED_TASK_ID));
    if (taskAttemptID == null) {
      taskAttemptID = new TaskAttemptID();
    }

    // create a TaskInputOutputContext
    Configuration conf = jobConf;
    if (skipTimestampConversion
        ^ HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
      HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION,
          skipTimestampConversion);
    }

    final TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(conf, taskAttemptID);
    if (parquetSplit != null) {
      realReader = newInputFormat.createRecordReader(parquetSplit, taskContext);
      realReader.initialize(parquetSplit, taskContext);
    } else {
      realReader = null;
    }
    if (valueObj == null) { // Should initialize the value for createValue
      valueObj = new ArrayWritable(Writable.class, new Writable[schemaSize]);
    }
  }



  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // if (realReader == null) {
    // return false;
    // }
    return realReader.nextKeyValue();
  }



  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }



  @Override
  public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
    System.out.println(StringUtils.join((Object[]) realReader.getCurrentValue().toArray(), ','));
    return realReader.getCurrentValue();
  }
}
