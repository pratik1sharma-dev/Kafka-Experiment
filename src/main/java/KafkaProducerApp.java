import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerApp
{
    public static void main(String[] args) throws InterruptedException
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092,localhost:9093");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties))
        {


            /*
            test_topic_1:  topic with multiple partitions on single brokre with single replication
            test_topic_2: Topic with multiple partitions on multiple brokers test_topic_2
            Order will not be maintained on consumer
            bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic_2
            */
            for (int i = 0; i < 100; i++)
            {
                kafkaProducer.send(new ProducerRecord<>("test_topic_1", "Hello_" + i));
                kafkaProducer.send(new ProducerRecord<>("test_topic_2", "Hello_" + i*100));
               Thread.sleep(5000);
            }


            //Data will be available in Consumer Record with both key and value.If no key provided it will be null
            //Key is also used here  to decide partition using hash
            kafkaProducer.send(new ProducerRecord<>("test_topic_2","1","Hello_j1"));
            kafkaProducer.send(new ProducerRecord<>("test_topic_2","2","Hello_j2"));
            kafkaProducer.send(new ProducerRecord<>("test_topic_2","1","Hello_j3"));

            //Partition Explicitly provided.
            kafkaProducer.send(new ProducerRecord<>("test_topic_2",2,"2","Hell"));

        }

    }

}
