package parquet.programs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import parquet.MapreduceParquetInputFormat;
import parquet.MapreduceParquetOutputFormat;
import scala.Tuple2;


/**
 * Description:
 *
 * OrcSparkReadWrite: Read and write orc. Just like a normal identity mapper.
 *
 * 
 *
 */
public class ParquetPartitioning {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ParquetSparkReadWrite <input> <output>");
      System.exit(1);
    }

    // handle input parameters
    final String inputPath = args[0];
    final String outputPath = args[1];

    // create a context object, which is used
    // as a factory for creating new RDDs
    String master = "local[1]";

    SparkConf conf =
        new SparkConf().setAppName(ParquetPartitioning.class.getName()).setMaster(master);
    JavaSparkContext ctx = new JavaSparkContext(conf);
    Configuration hadoopConfig = ctx.hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConfig);
    fs.delete(new Path(outputPath), true);
    Job job = Job.getInstance(hadoopConfig);
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.setOutputFormatClass(MapreduceParquetOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Writable.class);
    TypeInfo recordTypeInfo = getOrcSchema();
    job.getConfiguration().set("schema.string", recordTypeInfo.toString());
    // JavaPairRDD<NullWritable, ParquetHiveRecord> lines =
    // ctx.newAPIHadoopRDD(job.getConfiguration(),
    // MapreduceParquetInputFormat.class, NullWritable.class, ParquetHiveRecord.class);
    JavaPairRDD<NullWritable, ArrayWritable> lines =
        ctx.newAPIHadoopRDD(job.getConfiguration(),
            MapreduceParquetInputFormat.class, NullWritable.class, ArrayWritable.class);


    JavaPairRDD<NullWritable, Writable> op = lines
        .mapToPair(new PairFunction<Tuple2<NullWritable, ArrayWritable>, NullWritable, Writable>() {

          @Override
          public Tuple2<NullWritable, Writable> call(Tuple2<NullWritable, ArrayWritable> t)
              throws Exception {
            SerDe serde = new ParquetHiveSerDe();
            SettableStructObjectInspector oip =
                new ArrayWritableObjectInspector((StructTypeInfo) recordTypeInfo);
            return new Tuple2<>(NullWritable.get(), serde.serialize(t._2(), oip));

          }
        });
    op.saveAsNewAPIHadoopDataset(job.getConfiguration());

    // close the context and we are done
    ctx.close();
    System.exit(0);
  }

  public static TypeInfo getOrcSchema() throws Exception {
    String schema =
        "struct<id:bigint,rec_add_ts:timestamp,rec_chg_ts:timestamp,Col1:string,Col2:string,Col3:string,Col4:string,Col5:string,Col6:string,Col7:string,Col8:string,Col9:string,ZIW_ROW_ID:string,ZIW_SOURCE_START_DATE:date,ZIW_SOURCE_START_TIMESTAMP:timestamp,ZIW_TARGET_START_TIMESTAMP:timestamp,ZIW_TARGET_START_DATE:date,ZIW_SOURCE_END_DATE:date,ZIW_SOURCE_END_TIMESTAMP:timestamp,ZIW_TARGET_END_DATE:date,ZIW_TARGET_END_TIMESTAMP:timestamp,ZIW_ACTIVE:boolean,ZIW_IS_DELETED:boolean,ZIW_STATUS_FLAG:string>";
    // String schema =
    // "struct<ziw_row_id:string,ziw_scn:decimal(38,18),ziw_source_start_date:timestamp,ziw_source_start_timestamp:timestamp,ziw_target_start_timestamp:timestamp,ziw_target_start_date:timestamp,ziw_source_end_date:timestamp,ziw_source_end_timestamp:timestamp,ziw_target_end_date:timestamp,ziw_target_end_timestamp:timestamp,ziw_active:string,ziw_is_deleted:string,ziw_status_flag:string,id:decimal(38,0),rec_add_ts:timestamp,rec_chg_ts:timestamp,col1:string,col2:string,col3:string,col4:string,col5:string,col6:string,col7:string,col8:string,col9:string,col10:string,col11:string,col12:string,col13:string,col14:string,col15:string,col16:string,col17:string,col18:string,col19:string,col20:string,col21:string,col22:string,col23:string,col24:string,col25:string,col26:string,col27:string,col28:string,col29:string,col30:string,col31:string,col32:string,col33:string,col34:string,col35:string,col36:string,col37:string,col38:string,col39:string,col40:string,col41:string,col42:string,col43:string,col44:string,col45:string,col46:string,col47:string,col48:string,col49:string,col50:string,col51:string,col52:string,col53:string,col54:string,col55:string,col56:string,col57:string,col58:string,col59:string,col60:string,col61:string,col62:string,col63:string,col64:string,col65:string,col66:string,col67:string,col68:string,col69:string,col70:string,col71:string,col72:string,col73:string,col74:string,col75:string,col76:string,col77:string,col78:string,col79:string,col80:string,col81:string,col82:string,col83:string,col84:string,col85:string,col86:string,col87:string,col88:string,col89:string,col90:string,col91:string,col92:string,col93:string,col94:string,col95:string,col96:string,col97:string,col98:string,col99:string,col100:string,col101:string,col102:string,col103:string,col104:string,col105:string,col106:string,col107:string,col108:string,col109:string,col110:string,col111:string,col112:string,col113:string,col114:string,col115:string,col116:string,col117:string,col118:string,col119:string,col120:string,col121:string,col122:string,col123:string,col124:string,col125:string,col126:string,col127:string,col128:string,col129:string,col130:string,col131:string,col132:string,col133:string,col134:string,col135:string,col136:string,col137:string,col138:string,col139:string,col140:string,col141:string,col142:string,col143:string,col144:string,col145:string,col146:string,col147:string,col148:string,col149:string,col150:string>";
    return TypeInfoUtils.getTypeInfoFromTypeString(schema);
  }
}
