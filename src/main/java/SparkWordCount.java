

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

//
import scala.Tuple2;


/**
 * Description:
 *
 * SparkWordCount: Counting the words if their size is greater than or equal N, where N > 1.
 *
 * 
 *
 */
public class SparkWordCount {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: SparkWordCount <N> <input> <output>");
      System.exit(1);
    }

    // handle input parameters
    final int N = Integer.parseInt(args[0]);
    final String inputPath = args[1];
    final String outputPath = args[2];

    // create a context object, which is used
    // as a factory for creating new RDDs
    String master = "local[1]";

    SparkConf conf = new SparkConf().setAppName(SparkWordCount.class.getName()).setMaster(master);
    JavaSparkContext ctx = new JavaSparkContext(conf);
    Configuration hadoopConfig = ctx.hadoopConfiguration();
    FileSystem fs = FileSystem.get(hadoopConfig);
    fs.delete(new Path(outputPath), true);
    Job job = Job.getInstance(hadoopConfig);
    // read input and create the first RDD
    // TODO: use inputformat and conf
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    JavaPairRDD<LongWritable, Text> lines = ctx.newAPIHadoopRDD(job.getConfiguration(),
        TextInputFormat.class, LongWritable.class, Text.class);// textFile(inputPath,
    // 1);

    // output input output
    JavaPairRDD<String, Long> ones =
        lines.flatMapToPair(new PairFlatMapFunction<Tuple2<LongWritable, Text>, String, Long>() {
          // output input
          @Override
          public Iterator<Tuple2<String, Long>> call(Tuple2<LongWritable, Text> t)
              throws Exception {
            List<String> words = Util.convertStringToWords(t._2.toString(), N);
            List<Tuple2<String, Long>> tuples = new ArrayList<Tuple2<String, Long>>();
            for (String word : words) {
              tuples.add(new Tuple2<String, Long>(word, 1l));
            }
            return tuples.iterator();
          }
        });

    // // K V
    // JavaPairRDD<String, Long> ones =
    // // input K V
    // counts.mapToPair(new PairFunction<String, String, Long>() {
    // // K V input
    // @Override
    // public Tuple2<String, Long> call(String s) {
    // // K V
    // return new Tuple2<String, Long>(s, 1l);
    // }
    // });

    // find the total count for each unique word
    JavaPairRDD<String, Long> counts = ones.reduceByKey(new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long i1, Long i2) {
        return i1 + i2;
      }
    });

    // save the final output
    // TODO: use outputformat
    // counts.saveAsTextFile(outputPath);
    counts.saveAsNewAPIHadoopDataset(job.getConfiguration());

    // close the context and we are done
    ctx.close();

    System.exit(0);
  }

}
