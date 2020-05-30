import java.sql.SQLOutput;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerRunnable implements Runnable
{
    private KafkaConsumer kafkaConsumer;
    private String consumerName;
    public KafkaConsumerRunnable(String consumerName,Properties properties)
    {
       kafkaConsumer=new KafkaConsumer<String, String>(properties);
       this.consumerName=consumerName;
    }
    @Override
    public void run()
    {
        List<String> topics= new ArrayList<>();
        topics.add("test_topic_1");
       // topics.add("test_topic_2");
        System.out.println("Starting consumer "+consumerName);
        kafkaConsumer.subscribe(topics);
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(6));
         //   System.out.println(kafkaConsumer.listTopics());
           // System.out.println(consumerName+":" +kafkaConsumer.groupMetadata());

            for (ConsumerRecord<String, String> next : consumerRecords)
            {
                System.out.println(String.format("%s: topic=%s partition=%s value=%s offset=%s",consumerName,
                        next.topic(),
                        next.partition(),
                        next.value(),
                        next.offset()));
                try
                {

                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            System.out.println(consumerName);
        }


    }
}
