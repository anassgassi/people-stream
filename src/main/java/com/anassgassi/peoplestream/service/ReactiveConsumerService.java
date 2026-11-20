package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReactiveConsumerService {

    private static final long MAX_SAVE_RETRIES = 3;

    private final ReactiveKafkaConsumerTemplate<String, Person> reactiveKafkaConsumer;
    private final PersonRepository reactivePersonRepository;

    @PostConstruct
    public void init() {
        consume().subscribe(
                person -> log.info("Successfully processed: {}", person),
                error -> log.error("Stream terminated due to error: {}", error.getMessage()),
                () -> log.info("Stream completed")
        );
    }

    protected Flux<Person> consume() {
        return reactiveKafkaConsumer
                .receive()
                .concatMap(this::processRecord)
                // Resubscribe if the Kafka connection itself fails
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(signal -> log.warn("Restarting consumer: {}",
                                signal.failure().getMessage())))
                .publish()
                .autoConnect();
    }

    private Mono<Person> processRecord(ReceiverRecord<String, Person> consumerRecord) {
        logConsumerRecord(consumerRecord);

        Person person = consumerRecord.value();
        if (person == null) {
            log.warn("Skipping record with null value at offset={}", consumerRecord.offset());
            consumerRecord.receiverOffset().acknowledge();
            return Mono.empty();
        }

        // Acknowledge only after the save has finished (successfully or given up)
        return savePerson(person)
                .doOnTerminate(() -> consumerRecord.receiverOffset().acknowledge());
    }

    private void logConsumerRecord(ReceiverRecord<String, Person> consumerRecord) {
        log.info("Received key={}, value={} from topic={}, offset={}",
                consumerRecord.key(),
                consumerRecord.value(),
                consumerRecord.topic(),
                consumerRecord.offset());
    }

    private Mono<Person> savePerson(Person person) {
        return reactivePersonRepository.save(person)
                .retryWhen(Retry.backoff(MAX_SAVE_RETRIES, Duration.ofSeconds(1))
                        .filter(DataAccessException.class::isInstance))
                .doOnNext(savedPerson -> log.info("Saved person: {}", savedPerson))
                .onErrorResume(e -> {
                    log.error("Giving up saving person {}: {}", person, e.getMessage());
                    return Mono.empty();
                });
    }
}