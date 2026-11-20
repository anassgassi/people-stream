package com.anassgassi.peoplestream.repository;

import com.anassgassi.peoplestream.model.Person;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PersonRepository extends ReactiveElasticsearchRepository<Person, String> {
}
