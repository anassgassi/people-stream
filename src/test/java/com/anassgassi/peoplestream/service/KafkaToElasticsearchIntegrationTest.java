package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class KafkaToElasticsearchIntegrationTest {

    private static final String TOPIC = "anass-it";

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static ElasticsearchContainer elasticsearch =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.13.4"))
                    .withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.topic", () -> TOPIC);
        registry.add("spring.elasticsearch.uris", elasticsearch::getHttpHostAddress);
        registry.add("spring.data.elasticsearch.client.reactive.endpoints",
                () -> elasticsearch.getHost() + ":" + elasticsearch.getMappedPort(9200));
    }

    @Autowired
    private PersonRepository personRepository;

    @Test
    void messageSentToKafkaIsSavedInElasticsearch() {
        send("{\"name\":\"John Doe\",\"age\":30}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<Person> persons = personRepository.findAll().collectList().block();
            assertThat(persons)
                    .extracting(Person::getName, Person::getAge)
                    .contains(org.assertj.core.groups.Tuple.tuple("John Doe", 30));
        });
    }

    private void send(String json) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, "key", json));
            producer.flush();
        }
    }
}

