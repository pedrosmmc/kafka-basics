package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers = "localhost:9092";
        String groupId = "tutorials";
        String topic = "tutorial1";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = 15L;
        consumer.assign(Arrays.asList(topicPartition));

        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int messageCounter = 0;

        // seek
        consumer.seek(topicPartition, offset);

        // poll for new data
        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records) {
                messageCounter++;
                logger.info("\n\nkey: " + record.key() +
                      "\nvalue: " + record.value() +
                      "\npartition: " + record.partition() +
                      "\noffset: " + record.offset() + "\n\n");
                if(messageCounter >= numberOfMessagesToRead) {
                    keepReading = false; // to exit while loop
                    break;
                }
            }
        }
        logger.info("Exiting consumer...");
    }
}
