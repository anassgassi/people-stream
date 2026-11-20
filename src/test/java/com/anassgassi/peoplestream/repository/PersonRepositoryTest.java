package com.anassgassi.peoplestream.repository;

import com.anassgassi.peoplestream.ElasticsearchContainerSupport;
import com.anassgassi.peoplestream.model.Person;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import reactor.test.StepVerifier;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
public class PersonRepositoryTest extends ElasticsearchContainerSupport {

    @Autowired
    private PersonRepository personRepository;

    @Test
    public void savedPersonCanBeFoundById() {
        Person person = new Person();
        person.setId("1");
        person.setName("John Doe");
        person.setAge(30);

        StepVerifier.create(personRepository.save(person)
                        .then(personRepository.findById("1")))
                .expectNextMatches(found -> "John Doe".equals(found.getName()))
                .verifyComplete();
    }
}
