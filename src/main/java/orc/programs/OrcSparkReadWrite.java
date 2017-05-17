package orc.programs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Description:
 *
 * OrcSparkReadWrite: Read and write orc. Just like a normal identity mapper.
 *
 * 
 *
 */
public class OrcSparkReadWrite {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: OrcSparkReadWrite <input> <output>");
      System.exit(1);
    }

    // handle input parameters
    final String inputPath = args[1];
    final String outputPath = args[2];

    // create a context object, which is used
    // as a factory for creating new RDDs
    String master = "local[1]";

    SparkConf conf =
        new SparkConf().setAppName(OrcSparkReadWrite.class.getName()).setMaster(master);
    JavaSparkContext ctx = new JavaSparkContext(conf);
    Configuration hadoopConfig = ctx.hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConfig);
    fs.delete(new Path(outputPath), true);
    Job job = Job.getInstance(hadoopConfig);
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.setOutputFormatClass(OrcNewOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcStruct.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(OrcStruct.class);
    JavaPairRDD<NullWritable, OrcStruct> lines = ctx.newAPIHadoopRDD(job.getConfiguration(),
        OrcNewInputFormat.class, NullWritable.class, OrcStruct.class);// textFile(inputPath,
    lines.saveAsNewAPIHadoopDataset(job.getConfiguration());
    // close the context and we are done
    ctx.close();
    System.exit(0);
  }

}
