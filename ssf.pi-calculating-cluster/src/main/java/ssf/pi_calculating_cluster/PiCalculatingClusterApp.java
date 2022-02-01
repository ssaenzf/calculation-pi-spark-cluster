package ssf.pi_calculating_cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Compute Pi on a cluster.
 * 
 * It is not recommended to run this application from the IDE.
 * 
 * @author ssf
 */
public class PiCalculatingClusterApp implements Serializable {
  private static final long serialVersionUID = -1546L;
  private static long counter = 0;

  /**
   * Mapper class, creates the map of dots
   * 
   * @author ssf
   */
  private final class PointMapper
      implements MapFunction<Row, Integer> {
    private static final long serialVersionUID = 38446L;

    @Override
    public Integer call(Row r) throws Exception {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      counter++;
      if (counter % 1000 == 0) {
        System.out.println("" + counter + " operations done so far");
      }
      return (x * x + y * y <= 1) ? 1 : 0;
    }
  }

  /**
   * Reducer class, reduces the map of dots
   * 
   * @author ssf
   */
  private final class PointReducer implements ReduceFunction<Integer> {
    private static final long serialVersionUID = 12859L;

    @Override
    public Integer call(Integer x, Integer y) {
      return x + y;
    }
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
	  PiCalculatingClusterApp app = new PiCalculatingClusterApp();
    app.start(10);
  }

  /**
   * The processing code.
   */
  private void start(int slices) {
    int numberOfThrows = 100000 * slices;
    System.out.println("About to obtain " + numberOfThrows
        + " points, ready?");

    long t0 = System.currentTimeMillis();
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaSparkPi on a cluster")
        .master("spark://first_cluster:7077")
        .config("spark.executor.memory", "4g")
        .config("spark.jars", "C:\\EclipseEntornoTrabajo\\ssf.pi-calculating-cluster\\pi-calculating-cluster-1.0.0-SNAPSHOT.jar")
         
        .getOrCreate();

    long t1 = System.currentTimeMillis();
    System.out.println("Session initialized in " + (t1 - t0) + " ms");

    List<Integer> l = new ArrayList<>(numberOfThrows);
    for (int i = 0; i < numberOfThrows; i++) {
      l.add(i);
    }
    Dataset<Row> incrementalDf = spark
        .createDataset(l, Encoders.INT())
        .toDF();

    long t2 = System.currentTimeMillis();
    System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

    Dataset<Integer> pointsDs = incrementalDf
        .map(new PointMapper(), Encoders.INT());

    long t3 = System.currentTimeMillis();
    System.out.println("Obtaining points done in " + (t3 - t2) + " ms");

    int pointsInCircle = pointsDs.reduce(new PointReducer());
    long t4 = System.currentTimeMillis();
    System.out.println("Analyzing result in " + (t4 - t3) + " ms");

    System.out
        .println("Pi is roughly " + 4.0 * pointsInCircle / numberOfThrows);

    spark.stop();
  }
}
