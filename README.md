# kafka-test-harness

---

This program can be used to load multiple records to a kafka topic, in order to test the infrasturcture.

Varying parameters of such a load test:
1. Number of items being loaded, and the item size
2. Number of partitions existing for the topic
3. Replication factor of the topic
4. Asynchronous vs synchronous nature of message production

Runtime parameters
```
-Dkafka.test.no-of-producers=100
-Dkafka.test.items-per-producer=1000

-Dkafka.bootstrap-servers=hammer.nla.gov.au:19092
-Dkafka.topic=test.item.cdc.0

-Dkafka.producer.retries=0
-Dkafka.producer.acks=all

-Dkafka.properties.security.protocol=PLAINTEXT
-Dkafka.ssl.truststore-location=C:\\Users\\ashanbogh\\kafka.truststore.jks
-Dkafka.ssl.truststore-password=test1234
-Dkafka.ssl.keystore-location=C:\\Users\\ashanbogh\\kafka.keystore.jks
-Dkafka.ssl.keystore-password=test1234
-Dkafka.ssl.key-password=test1234
```
