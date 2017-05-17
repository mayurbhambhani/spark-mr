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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import parquet.hadoop.ParquetOutputFormat;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapreduceParquetOutputFormat extends
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<NullWritable, ParquetHiveRecord> {

  private static final Log LOG = LogFactory.getLog(MapreduceParquetOutputFormat.class);

  protected ParquetOutputFormat<ParquetHiveRecord> realOutputFormat;

  public MapreduceParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ParquetHiveRecord>(new DataWritableWriteSupport());
  }

  public MapreduceParquetOutputFormat(
      final OutputFormat<Void, ParquetHiveRecord> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ParquetHiveRecord>) mapreduceOutputFormat;
  }



  protected ParquetRecordWriterWrapper getParquerRecordWriterWrapper(
      ParquetOutputFormat<ParquetHiveRecord> realOutputFormat, Configuration jobConf,
      String finalOutPath) throws IOException {
    return new ParquetRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString());
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordWriter<NullWritable, ParquetHiveRecord> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    LOG.info("creating new record writer..." + this);

    final String parquet = job.getConfiguration().get("schema.string");
    TypeInfo typeinfo = TypeInfoUtils.getTypeInfoFromTypeString(parquet);
    List<String> columnNames = new ArrayList<String>();
    List<TypeInfo> columnTypes = new ArrayList<TypeInfo>();
    SettableStructObjectInspector soip =
        (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeinfo);
    List<StructField> fields = (List<StructField>) soip.getAllStructFieldRefs();
    for (StructField field : fields) {
      columnNames.add(field.getFieldName().toLowerCase());
      columnTypes
          .add(TypeInfoUtils.getTypeInfoFromObjectInspector(field.getFieldObjectInspector()));
    }
    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes),
        job.getConfiguration());
    Path file = getDefaultWorkFile(job, "");
    return getParquerRecordWriterWrapper(realOutputFormat, job.getConfiguration(), file.toString());
  }
}
