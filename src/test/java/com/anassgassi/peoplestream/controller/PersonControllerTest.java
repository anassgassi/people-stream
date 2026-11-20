package com.anassgassi.peoplestream.controller;

import com.anassgassi.peoplestream.model.Person;
import com.anassgassi.peoplestream.service.PersonService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest(PersonController.class)
class PersonControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private PersonService personService;

    private Person person(String id, String name, int age) {
        Person person = new Person();
        person.setId(id);
        person.setName(name);
        person.setAge(age);
        return person;
    }

    @Test
    void getAllPersonsReturnsList() {
        when(personService.findAll()).thenReturn(Flux.just(person("1", "John", 30), person("2", "Jane", 25)));

        webTestClient.get().uri("/api/persons")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Person.class).hasSize(2);
    }

    @Test
    void getPersonByIdReturnsOkWhenFound() {
        when(personService.findById("1")).thenReturn(Mono.just(person("1", "John", 30)));

        webTestClient.get().uri("/api/persons/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("John");
    }

    @Test
    void getPersonByIdReturnsNotFoundWhenMissing() {
        when(personService.findById("1")).thenReturn(Mono.empty());

        webTestClient.get().uri("/api/persons/1")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void createPersonReturnsSavedPerson() {
        when(personService.create(any(Person.class))).thenReturn(Mono.just(person("1", "John", 30)));

        webTestClient.post().uri("/api/persons")
                .bodyValue("{\"name\":\"John\",\"age\":30}")
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo("1");
    }

    @Test
    void createPersonReturnsBadRequestWhenNameIsBlank() {
        webTestClient.post().uri("/api/persons")
                .bodyValue("{\"name\":\"\",\"age\":30}")
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void updatePersonReturnsOkWhenFound() {
        when(personService.update(eq("1"), any(Person.class))).thenReturn(Mono.just(person("1", "Johnny", 31)));

        webTestClient.put().uri("/api/persons/1")
                .bodyValue("{\"name\":\"Johnny\",\"age\":31}")
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("Johnny");
    }

    @Test
    void updatePersonReturnsNotFoundWhenMissing() {
        when(personService.update(eq("1"), any(Person.class))).thenReturn(Mono.empty());

        webTestClient.put().uri("/api/persons/1")
                .bodyValue("{\"name\":\"Johnny\",\"age\":31}")
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void deletePersonReturnsOkWhenFound() {
        when(personService.delete("1")).thenReturn(Mono.just(true));

        webTestClient.delete().uri("/api/persons/1")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void deletePersonReturnsNotFoundWhenMissing() {
        when(personService.delete("1")).thenReturn(Mono.just(false));

        webTestClient.delete().uri("/api/persons/1")
                .exchange()
                .expectStatus().isNotFound();
    }
}

