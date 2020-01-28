package at.jku.dke.dwh.spark;


import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a stream from a stream (files) and
 * 
 * @author jgp
 */
public class ReadOrdersFromFileStreamApp implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 4317606738274123213L;
  
  private static Logger log = LoggerFactory
      .getLogger(ReadOrdersFromFileStreamApp.class);

  public static void main(String[] args) {
    ReadOrdersFromFileStreamApp app = new ReadOrdersFromFileStreamApp();
    app.start();
  }
  
  class OrderMapper implements MapFunction<Row, Order> {
    private static final long serialVersionUID = -2L;

    @Override
    public Order call(Row value) throws Exception {
      Order o = new Order();
      
      o.setId(value.getAs("id"));
      o.setClientId(value.getAs("clientId"));
      o.setStockSymbol(value.getAs("stockSymbol"));
      o.setStocksSold(value.getAs("stocksSold"));
      o.setPrice(value.getAs("price"));
      o.setAction(value.getAs("action"));

      // we use java.sql.Timestamp to represent the timestamp
      String dateAsString = value.getAs("timestamp");
      if (dateAsString != null) {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        o.setTimestamp(new java.sql.Timestamp(parser.parse(dateAsString).getTime()));
      }
      return o;
    }
  }

  private void start() {
    log.debug("-> start()");

    SparkSession spark = SparkSession.builder()
        .appName("Read lines from a file stream")
        .master("local[*]")
        .getOrCreate();
    
    // deifne the type of the dataframe
    StructType schema = new StructType()
        .add("timestamp","string")
        .add("id","int")
        .add("clientId","int")
        .add("stockSymbol", "string")
        .add("stocksSold", "int")
        .add("price", "double")
        .add("action", "string");
    
    Dataset<Row> df = spark
        // we create a streaming dataframe
        .readStream()
        .option("sep", ",")
        .schema(schema)
        // read the files from a directory --> change to SplitAndSend's output directory
        .csv("./orders/");

    // print the schema of the streaming dataframe
    df.printSchema();
    
    // convert the dataframe into a dataset of order objects, using the mapper defined above
    Dataset<Order> orders = df.map(
        new OrderMapper(), 
        Encoders.bean(Order.class)
    );
    
    // now we aggregate. we use the order dataset as a start.
    // Java is more clumsy than Scala when it comes to functional programming.
    Dataset<Row> dfGlobalAgg = orders
      // The parameter of groupByKey is a function, which takes an Order value as input 
      // and returns a string --> we group by the stock symbol
      .groupByKey((MapFunction<Order, String>) value -> value.getStockSymbol(), Encoders.STRING())
      // we then aggregate using avg on the price
      .agg(typed.avg((MapFunction<Order, Double>) value -> value.getPrice()))
      // we convert to a dataframe, for ease of use
      .toDF()
      // we order by value, which is the column name given by the aggregate function
      .orderBy("value");
    
    dfGlobalAgg.printSchema();
    
    // We create a continuous query over the aggregate dataframe
    // The query's sink is the console: It will output the result
    // periodically when finished with processing a microbatch.
    StreamingQuery queryGlobalAgg = dfGlobalAgg
        .writeStream()
        .outputMode(OutputMode.Complete())
        .format("console")
        .start();
    
    // We now perform a windowed query
    Dataset<Row> dfWindowedAgg = orders
      // for each stock, we compute windows of 30 seconds, updated every 10 seconds
      .groupBy(
          functions.window(orders.col("timestamp"), "30 seconds", "10 seconds"), 
          orders.col("stockSymbol")
      )
      // again, we aggregate the price
      .agg(typed.avg((MapFunction<Row, Double>) value -> value.getAs("price")))
      .orderBy("stockSymbol", "window");
    
    dfWindowedAgg.printSchema();
    
    
    // Create a query for the windowed dataframe as well
    StreamingQuery queryWindowedAgg = dfWindowedAgg
        .writeStream()
        .outputMode(OutputMode.Complete())
        .format("console")
        .start();
    
    try {
      // wait for the queries to return results
      queryGlobalAgg.awaitTermination();
      queryWindowedAgg.awaitTermination();
    } catch (StreamingQueryException e) {
      log.error(
          "Exception while waiting for query to end {}.",
          e.getMessage(),
          e);
    }

    log.debug("<- start()");
  }
}
