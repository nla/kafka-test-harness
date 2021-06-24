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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.*;

public class KafkaTestHarness {

	Logger logger = LoggerFactory.getLogger(KafkaTestHarness.class.getName());

	public static void main(String args[]) throws IOException, InterruptedException {
	    new KafkaTestHarness().loadKafka();
    }

    volatile long totalRecordsProccessed;
	volatile boolean isForcedShutdown = false;

	public void loadKafka() throws IOException, InterruptedException {

	    String itemXMLTemplate = loadXMLTemplate();

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

        List<ItemProducer> listProducers = new ArrayList<>();
        List<Future> listFutures = new ArrayList<>();

        ExecutorService es = Executors.newCachedThreadPool();

        for(int i = 1; i <= NO_OF_PRODUCER_THREADS; i++) {
            ItemProducer itemProducer = new ItemProducer(properties, topic, "Thread_" + i, NO_OF_ITEMS_PER_PRODUCER_THREAD, itemXMLTemplate);
            listProducers.add(itemProducer);

            Future future = es.submit(itemProducer);
            listFutures.add(future);
        }

        Thread shutdownHook = new Thread(() -> {
            isForcedShutdown = true;

            logger.info("Caught shutdown hook");

            for(ItemProducer itemProducer: listProducers) {
                itemProducer.setShutdown(true);
            }

            logger.info("Total number of records produced ---> {}", collect(listFutures));
        });

        //add a shutdown hook to close Item Consumer
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        totalRecordsProccessed = collect(listFutures);

        logger.info("Total number of records scheduled ---> {} Threads X {} Items per thread = {} Items", NO_OF_PRODUCER_THREADS, NO_OF_ITEMS_PER_PRODUCER_THREAD, NO_OF_PRODUCER_THREADS * NO_OF_ITEMS_PER_PRODUCER_THREAD);
        es.shutdown();

        if(!isForcedShutdown) {
            logger.info("Total number of records produced ---> {}", totalRecordsProccessed);
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }

        logger.info("Application has exited");
    }

    private String loadXMLTemplate() throws IOException {

	    String itemXMLTemplate;

        // Read Marc item template
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

        return itemXMLTemplate;
    }

    private long collect(List<Future> listFutures) {

        long totalRecordsProccessed = 0;

        for(Future future:listFutures) {

            try {
                totalRecordsProccessed += (int)future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return totalRecordsProccessed;
    }
}
