package text;



import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * Description:
 *
 * SparkWordCount: Counting the words if their size is greater than or equal N, where N > 1.
 *
 * 
 *
 */
public class SparkParquetReadWriteDataSet {

  @SuppressWarnings({"rawtypes", "serial", "unchecked"})
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: SparkParquetToText  <input> <output>");
      System.exit(1);
    }
    String master = "local[1]";
    SparkSession spark = SparkSession.builder().appName("Java Spark SQL Example").master(master)
        .config("spark.some.config.option", "some-value").getOrCreate();
    Dataset<Row> ds = spark.read().parquet(args[0]);
    StructType x = ds.schema();
    StructField[] fields = x.fields();
    for (StructField structField : fields) {
      System.out.println(structField.name() + ":" + structField.dataType().typeName());
    }
    // ds.show();
    Dataset<Row> ds_o = ds.map(new MapFunction() {

      @Override
      public Row call(Object value) throws Exception {
        return (Row) value;
      }
    }, Encoders.bean(Row.class));
    System.out.println(ds.count());
    ds.write().parquet(args[1]);
  }

}

