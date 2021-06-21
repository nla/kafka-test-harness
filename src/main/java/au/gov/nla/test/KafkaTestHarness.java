package au.gov.nla.test;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaTestHarness {

	Logger logger = LoggerFactory.getLogger(KafkaTestHarness.class.getName());

	public static void main(String args[]) throws IOException, InterruptedException {
	    new KafkaTestHarness().init();
    }

	public void init() throws IOException, InterruptedException {

        // Read Marc item template
        String itemXMLTemplate;
        InputStream inputStream = getClass().getResourceAsStream("/item_test_record.xml");
        try(BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            itemXMLTemplate = sb.toString();
        }

        logger.info("### Template read -> \n{}", itemXMLTemplate);

        int NO_OF_PRODUCER_THREADS = Integer.parseInt(System.getProperty("kafka.test.no-of-producers"));
        int NO_OF_ITEMS_PER_PRODUCER_THREAD = Integer.parseInt(System.getProperty("kafka.test.items-per-producer"));
        String topic = System.getProperty("kafka.topic");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("kafka.bootstrap-servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG, System.getProperty("kafka.producer.retries"));
        properties.put(ProducerConfig.ACKS_CONFIG, System.getProperty("kafka.producer.acks"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, System.getProperty("kafka.properties.security.protocol"));
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, System.getProperty("kafka.ssl.truststore-location"));
        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, System.getProperty("kafka.ssl.truststore-password"));
        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, System.getProperty("kafka.ssl.keystore-location"));
        properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, System.getProperty("kafka.ssl.keystore-password"));
        properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, System.getProperty("kafka.ssl.key-password"));

        ExecutorService es = Executors.newCachedThreadPool();
        for(int i = 1; i <= NO_OF_PRODUCER_THREADS; i++) {
            es.execute(new ItemProducer(properties, topic, "Thread_" + i, NO_OF_ITEMS_PER_PRODUCER_THREAD, itemXMLTemplate));
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);

        logger.info("Total number of records produced ---> {} Threads X {} Items per thread = {} Items", NO_OF_PRODUCER_THREADS, NO_OF_ITEMS_PER_PRODUCER_THREAD, NO_OF_PRODUCER_THREADS * NO_OF_ITEMS_PER_PRODUCER_THREAD);
    }
}
