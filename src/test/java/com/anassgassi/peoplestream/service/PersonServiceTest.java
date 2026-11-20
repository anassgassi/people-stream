package com.anassgassi.peoplestream.service;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.repository.PersonRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PersonServiceTest {

    @Mock
    private PersonRepository personRepository;

    @InjectMocks
    private PersonService personService;

    private Person person(String id, String name, int age) {
        Person person = new Person();
        person.setId(id);
        person.setName(name);
        person.setAge(age);
        return person;
    }

    @Test
    void findAllReturnsAllPersons() {
        Person john = person("1", "John", 30);
        Person jane = person("2", "Jane", 25);
        when(personRepository.findAll()).thenReturn(Flux.just(john, jane));

        StepVerifier.create(personService.findAll())
                .expectNext(john, jane)
                .verifyComplete();
    }

    @Test
    void findByIdReturnsPersonWhenFound() {
        Person john = person("1", "John", 30);
        when(personRepository.findById("1")).thenReturn(Mono.just(john));

        StepVerifier.create(personService.findById("1"))
                .expectNext(john)
                .verifyComplete();
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        when(personRepository.findById("1")).thenReturn(Mono.empty());

        StepVerifier.create(personService.findById("1"))
                .verifyComplete();
    }

    @Test
    void createSavesAndReturnsPerson() {
        Person john = person(null, "John", 30);
        Person saved = person("1", "John", 30);
        when(personRepository.save(john)).thenReturn(Mono.just(saved));

        StepVerifier.create(personService.create(john))
                .expectNext(saved)
                .verifyComplete();
    }

    @Test
    void updateCopiesNameAndAgeToExistingPerson() {
        Person existing = person("1", "John", 30);
        Person changes = person(null, "Johnny", 31);
        when(personRepository.findById("1")).thenReturn(Mono.just(existing));
        when(personRepository.save(any(Person.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(personService.update("1", changes))
                .expectNextMatches(updated -> "1".equals(updated.getId())
                        && "Johnny".equals(updated.getName())
                        && Integer.valueOf(31).equals(updated.getAge()))
                .verifyComplete();
    }

    @Test
    void updateReturnsEmptyAndDoesNotSaveWhenPersonNotFound() {
        when(personRepository.findById("1")).thenReturn(Mono.empty());

        StepVerifier.create(personService.update("1", person(null, "Johnny", 31)))
                .verifyComplete();

        verify(personRepository, never()).save(any(Person.class));
    }

    @Test
    void deleteReturnsTrueWhenPersonExists() {
        Person john = person("1", "John", 30);
        when(personRepository.findById("1")).thenReturn(Mono.just(john));
        when(personRepository.delete(john)).thenReturn(Mono.empty());

        StepVerifier.create(personService.delete("1"))
                .expectNext(true)
                .verifyComplete();

        verify(personRepository).delete(john);
    }

    @Test
    void deleteReturnsFalseAndDoesNotDeleteWhenPersonNotFound() {
        when(personRepository.findById("1")).thenReturn(Mono.empty());

        StepVerifier.create(personService.delete("1"))
                .expectNext(false)
                .verifyComplete();

        verify(personRepository, never()).delete(any(Person.class));
    }
}

