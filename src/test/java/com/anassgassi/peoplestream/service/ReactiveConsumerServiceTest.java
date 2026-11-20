package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;

class ReactiveConsumerServiceTest {

    private ReactiveConsumerService reactiveConsumerService;
    private ReactiveKafkaConsumerTemplate<String, Person> reactiveKafkaConsumer;
    private PersonRepository personRepository;
    private ReceiverOffset receiverOffset;

    @BeforeEach
    void setup() {
        reactiveKafkaConsumer = Mockito.mock(ReactiveKafkaConsumerTemplate.class);
        personRepository = Mockito.mock(PersonRepository.class);
        receiverOffset = Mockito.mock(ReceiverOffset.class);
        reactiveConsumerService = new ReactiveConsumerService(reactiveKafkaConsumer, personRepository);
    }

    private ReceiverRecord<String, Person> receiverRecord(Person person) {
        return new ReceiverRecord<>(new ConsumerRecord<>("topic", 0, 0, "key", person), receiverOffset);
    }

    @Test
    @DisplayName("Consume and process valid Person successfully")
    void consumeAndProcessValidPersonSuccessfully() {
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
    @DisplayName("Handle null Person gracefully")
    void handleNullPersonGracefully() {
        Mockito.when(reactiveKafkaConsumer.receive())
                .thenReturn(Flux.just(receiverRecord(null)));

        StepVerifier.create(reactiveConsumerService.consume())
                .expectNextCount(0)
                .verifyComplete();

        Mockito.verifyNoInteractions(personRepository);
        Mockito.verify(receiverOffset).acknowledge();
    }

    @Test
    @DisplayName("Handle database exception during save")
    void handleDatabaseExceptionDuringSave() {
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