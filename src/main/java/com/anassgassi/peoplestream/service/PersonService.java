package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PersonService {

    private final PersonRepository personRepository;

    public Flux<Person> findAll() {
        return personRepository.findAll();
    }

    public Mono<Person> findById(String id) {
        return personRepository.findById(id);
    }

    public Mono<Person> create(Person person) {
        return personRepository.save(person);
    }

    public Mono<Person> update(String id, Person person) {
        return personRepository.findById(id)
                .flatMap(existingPerson -> {
                    existingPerson.setName(person.getName());
                    existingPerson.setAge(person.getAge());
                    return personRepository.save(existingPerson);
                });
    }

    public Mono<Boolean> delete(String id) {
        return personRepository.findById(id)
                .flatMap(existingPerson -> personRepository.delete(existingPerson).thenReturn(true))
                .defaultIfEmpty(false);
    }
}

