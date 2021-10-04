package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerDemoKeys {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "tutorial1";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        IntStream.rangeClosed(1,10).forEach(i -> {
            String key = "id_" + Integer.toString(i);
            String value = "hello world@" + Integer.toString(i);

            // create a Producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key: " + key);

            // send data with callback- asynchronous
            try {
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
                }).get(); // block the .send() to make it synchronous just for testing
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        // we must flush or close the producer so the record is really sent
//        producer.flush();
        producer.close();
    }
}
