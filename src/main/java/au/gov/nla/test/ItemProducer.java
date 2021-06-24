package au.gov.nla.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;

public class ItemProducer implements Callable {

	Logger logger = LoggerFactory.getLogger(ItemProducer.class.getName());

	private volatile boolean shutdown;

	private KafkaProducer<String, String> producer;

	private String threadName;
	private static String topic;
	private static int noOfItemsToCreate;

	public static String itemXMLTemplate;
    public static String TEMPLATE_KEY_COLLECTION_ID = "###KEY_COLLECTION_ID###";

	public ItemProducer(Properties properties, String topic, String threadName, int noOfItemsToCreate, String itemXMLTemplate) {

        producer = new KafkaProducer<>(properties);
        this.threadName = threadName;
        this.noOfItemsToCreate = noOfItemsToCreate;
        this.topic = topic;
        this.itemXMLTemplate = itemXMLTemplate;
        logger.debug("--> Item Producer, Kafka configurations = {}, topic={}, threadName={}, noOfItemsToCreate={}", properties, topic, threadName, noOfItemsToCreate);
    }

    @Override
    public Integer call() {

	    try {
            logger.info("########### Starting ItemProducer - {} #############", threadName);

            for (int i = 1; i <= noOfItemsToCreate; i++) {

                String message = itemXMLTemplate.replace(TEMPLATE_KEY_COLLECTION_ID, threadName + "_Item_" + i);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, threadName, itemXMLTemplate);
                producer.send(record);

                if(i%10 == 0) {
                    producer.flush();
                    if(this.shutdown) {
                        return i;
                    }
                }
            }

            return noOfItemsToCreate;
        }
	    finally {
            producer.flush();
            producer.close();
        }
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }
}

