package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
class ReactiveConsumerServiceIntegrationTest {

    @TestConfiguration
    static class KafkaConsumerMockConfig {
        @Bean
        @Primary
        @SuppressWarnings("unchecked")
        ReactiveKafkaConsumerTemplate<String, Person> mockedKafkaConsumer() {
            ReactiveKafkaConsumerTemplate<String, Person> consumer = Mockito.mock(ReactiveKafkaConsumerTemplate.class);
            // The service subscribes on startup, so the mock needs a stream before any test runs
            Mockito.when(consumer.receive()).thenReturn(Flux.never());
            return consumer;
        }
    }

    @Autowired
    private ReactiveConsumerService reactiveConsumerService;

    @Autowired
    private ReactiveKafkaConsumerTemplate<String, Person> reactiveKafkaConsumer;

    @MockBean
    private PersonRepository personRepository;

    private ReceiverOffset receiverOffset;

    @BeforeEach
    void setup() {
        Mockito.reset(reactiveKafkaConsumer, personRepository);
        receiverOffset = Mockito.mock(ReceiverOffset.class);
    }

    private ReceiverRecord<String, Person> receiverRecord(Person person) {
        return new ReceiverRecord<>(new ConsumerRecord<>("topic", 0, 0, "key", person), receiverOffset);
    }

    @Test
    @DisplayName("Successfully process and save a valid Person")
    void successfullyProcessAndSavePerson() {
        Person person = new Person();
        person.setName("John Doe");
        person.setAge(30);

        Mockito.when(reactiveKafkaConsumer.receive())
                .thenReturn(Flux.just(receiverRecord(person)));
        Mockito.when(personRepository.save(any(Person.class)))
                .thenReturn(Mono.just(person));

        StepVerifier.create(reactiveConsumerService.consume())
                .expectNext(person)
                .verifyComplete();

        Mockito.verify(personRepository).save(person);
        Mockito.verify(receiverOffset).acknowledge();
    }

    @Test
    @DisplayName("Handle database error gracefully")
    void handleDatabaseErrorGracefully() {
        Person person = new Person();
        person.setName("Jane Doe");
        person.setAge(25);

        Mockito.when(reactiveKafkaConsumer.receive())
                .thenReturn(Flux.just(receiverRecord(person)));
        Mockito.when(personRepository.save(any(Person.class)))
                .thenReturn(Mono.error(new RuntimeException("Database error")));

        StepVerifier.create(reactiveConsumerService.consume())
                .expectNextCount(0)
                .verifyComplete();

        Mockito.verify(personRepository).save(person);
        Mockito.verify(receiverOffset).acknowledge();
    }

    @Test
    @DisplayName("Handle invalid Person data gracefully")
    void handleInvalidPersonDataGracefully() {
        Mockito.when(reactiveKafkaConsumer.receive())
                .thenReturn(Flux.just(receiverRecord(null)));

        StepVerifier.create(reactiveConsumerService.consume())
                .expectNextCount(0)
                .verifyComplete();

        Mockito.verifyNoInteractions(personRepository);
        Mockito.verify(receiverOffset).acknowledge();
    }

    @Test
    @DisplayName("Handle empty Kafka stream")
    void handleEmptyKafkaStream() {
        Mockito.when(reactiveKafkaConsumer.receive())
                .thenReturn(Flux.empty());

        StepVerifier.create(reactiveConsumerService.consume())
                .expectNextCount(0)
                .verifyComplete();

        Mockito.verifyNoInteractions(personRepository);
    }
}