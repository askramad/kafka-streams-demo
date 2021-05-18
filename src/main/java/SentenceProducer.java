import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class SentenceProducer {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","127.0.0.1:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(kafkaProps);




        ProducerRecord<String, String> record1 = new ProducerRecord("words-source",
                    "1",
                    "Kafka est utilisé principalement pour la mise en place de « data pipeline »\n");
        ProducerRecord<String, String> record2 = new ProducerRecord("words-source",
                "2",
                "temps réel mais ce n'est pas sa seule application possible dans le monde de l'entreprise.\n");
        ProducerRecord<String, String> record3 = new ProducerRecord("words-source",
                "3",
                "Il est aussi de plus en plus utilisé dans les architectures micro services comme système d’échange,\n");
        ProducerRecord<String, String> record4 = new ProducerRecord("words-source",
                "4",
                "dans la supervision temps réel et dans l’IOT14. Kafka apporte sa capacité à ingérer et diffuser une\n");

            try {

                producer.send(record1);
                producer.send(record2);
                producer.send(record3);
                producer.send(record4);

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.flush();
    }
}
