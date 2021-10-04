package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("tutorial1", "hello world");

        // send data with callback- asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes everytime a record is successfully sent or and exception is sent
                if (e == null) {
                    // the record was successfully sent
                    logger.info("\nReceived new metadata." +
                          "\nTopic: " + recordMetadata.topic() +
                          "\nPartition: " + recordMetadata.partition() +
                          "\nOffset: " + recordMetadata.offset() +
                          "\nTimestamp: " + recordMetadata.timestamp());

                    logger.info(recordMetadata.toString());
                } else {
                    logger.error("Error while producing: " + e);
                }
            }
        });

        // we must flush or close the producer so the record is really sent
//        producer.flush();
        producer.close();
    }
}
