package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    String consumerKey = "2IhfJa0eSfDRNHaAeCTf9if0C";
    String consumerSecret = "qBFiaQo4cXvbo92MuQmglrOv638Cpg0Q7GsV7DX0citJtc59Sq";
    String token = "20352046-2MERFdpgGwTOIno30SEPivqlgank6cNabnqVrAhHk";
    String secret = "QIXodC4peo4mMmC7WXuwPZyBuJacfhQPftXky2y4T2toU";

    List<String> terms = Lists.newArrayList("kafka","AMC", "Ravencoin", "Bitcoin", "Citadel", "Cristiano Ronaldo", "Tesla");

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        
        // create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create a Kafka producer
        /** kafka-topics --zookeeper 127.0.0.1:2181 --topic twitter_tweets --create --partitions 6 --replication-factor 1 */
        String topic = "twitter_tweets";
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping " + TwitterProducer.class.toString() + "...");
            logger.info("Shutting down client from twitter...");
            client.stop();
            logger.info("Closing Kafka producer...");
            producer.close();
            logger.info("Done!");
        }));

        // loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null) {
                JSONObject jsonObject = new JSONObject(msg);
                String key = jsonObject.getString("id_str");


                ProducerRecord<String, String> record = new ProducerRecord(topic, null, msg);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            logger.error("Couldn\'t record the tweet to Kafka!", e);
                        }
                    }
                });
//                producer.flush();
                logger.info("Added tweet "+ key + " to Kafka");
//                logger.info("username: " + jsonObject.getJSONObject("user").getString("name"));
            }
        }
        logger.info(TwitterProducer.class.getName() + " ended.");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
              .name("Hosebird-Client-01") // optional: mainly for the logs
              .hosts(hosebirdHosts)
              .authentication(hosebirdAuth)
              .endpoint(hosebirdEndpoint)
              .processor(new StringDelimitedProcessor(msgQueue))
              /*.eventMessageQueue(eventQueue)*/; // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size

        // create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // for Kafka >=1.1

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
