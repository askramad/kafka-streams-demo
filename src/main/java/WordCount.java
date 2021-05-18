import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import scala.collection.immutable.Stream;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) throws InterruptedException {

        final StreamsBuilder builder = new StreamsBuilder();

        Properties kafkaProps = new Properties();

        kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        kafkaProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        final KStream<String,String> textLines = builder.stream("words-source");
        textLines.flatMapValues(value  -> Arrays.asList(value.toLowerCase().split("\\W+"))).
                groupBy( (key,value) -> value).count(Materialized.as("wordCount")).toStream().to("\"words-sinK\"");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(kafkaProps),kafkaProps);

        kafkaStreams.cleanUp();

        kafkaStreams.start();

        Thread.sleep(1000);

        kafkaStreams.close();






    }

}
