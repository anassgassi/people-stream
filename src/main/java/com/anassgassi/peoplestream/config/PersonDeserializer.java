package com.anassgassi.peoplestream.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.anassgassi.peoplestream.model.Person;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Person deserialize(String s, byte[] data) {
        try {
            return objectMapper.readValue(data, Person.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Person object", e);
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
