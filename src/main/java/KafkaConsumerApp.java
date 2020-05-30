import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.CollectionUtils;

import sun.security.util.ArrayUtil;

public class KafkaConsumerApp
{
    /*
    Rerun this app with same group id, you will observe some partitions are not assigned to any consumer.
    TODO:: Look for whats timeout if all consumer goes down in a group.
     */
    public static void main(String[] args) throws InterruptedException
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092,localhost:9093");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id","mygroup2");


       /* try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties))
        {
        ///    Experiment1(kafkaConsumer);

        }
*/

        for(int i=0;i<2;i++){
                KafkaConsumerRunnable kafkaConsumerRunnable= new KafkaConsumerRunnable("Consumer "+i,
                        properties);
                new Thread(kafkaConsumerRunnable).start();
        }


    }

    /*
    * This method will read from a single partition from any topic.
    * Ex. if Group coorodinate assigns this consumer partition 2. It can read for partition 2 for  both topics
    * */
    public static void Experiment1(KafkaConsumer kafkaConsumer) throws InterruptedException{
        //topic with multiple partitions on multiple brokers
        List<String> topics= new ArrayList<>();
        topics.add("test_topic_1");
        topics.add("test_topic_2");

        kafkaConsumer.subscribe(topics);
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> next : consumerRecords)
            {
                System.out.println(next);
                Thread.sleep(1000);
            }
            System.out.println();
        }
    }

}
