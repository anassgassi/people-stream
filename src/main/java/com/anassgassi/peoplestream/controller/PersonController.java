package com.anassgassi.peoplestream.controller;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.service.PersonService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/persons")
@RequiredArgsConstructor
public class PersonController {

    private final PersonService personService;

    @GetMapping
    public Flux<Person> getAllPersons() {
        return personService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Person>> getPersonById(@PathVariable String id) {
        return personService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    public Mono<Person> createPerson(@Valid @RequestBody Person person) {
        return personService.create(person);
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<Person>> updatePerson(@PathVariable String id,
                                                     @Valid @RequestBody Person person) {
        return personService.update(id, person)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> deletePerson(@PathVariable String id) {
        return personService.delete(id)
                .map(deleted -> deleted
                        ? ResponseEntity.ok().<Void>build()
                        : ResponseEntity.notFound().<Void>build());
    }
}