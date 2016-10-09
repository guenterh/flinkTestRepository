package gh;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by swissbib on 26.09.16.
 */
public class FlinkKafkaTest {

    public static void main (String[] args) throws Exception {

        //final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "swissbib.oai");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.setProperty("au  to.offset.reset","smallest");


        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer<String>("OAI",new SimpleStringSchema(),properties,
                        FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
                        FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));



        messageStream.map(new MapFunction<String, String>() {
            long iteratons = 0;
            @Override
            public String map(String s) throws Exception {
                iteratons++;
                //return Long.toString(iteratons);
                return s;
            }
        }).rebalance().print();

        env.execute("Read from Kafka example");





    }
}