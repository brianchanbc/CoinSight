import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamPrice {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()


  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("brianchan_crypto_prices_minimal_hbase"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamPrice")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("brianchan_crypto_topic")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Read the lines
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[CryptoPriceReport]))

    // Write to an HBase table
    val batchStats = reports.map(snapshot => {
      try {
        val pairDate = s"${snapshot.pair}_${snapshot.price_date}"
        val put = new Put(Bytes.toBytes(pairDate))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pair"), Bytes.toBytes(snapshot.pair))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("price_date"), Bytes.toBytes(snapshot.price_date))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("close"), Bytes.toBytes(snapshot.close))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("volume"), Bytes.toBytes(snapshot.volume))
        table.put(put)
      } catch {
        case e: Exception =>
          System.err.println(s"Failed to write to HBase: ${e.getMessage}")
      }
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
