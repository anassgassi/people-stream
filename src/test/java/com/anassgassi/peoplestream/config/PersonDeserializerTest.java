package com.anassgassi.peoplestream.config;

import org.junit.jupiter.api.Test;

public class PersonDeserializerTest {

    private PersonDeserializer personDeserializer = new PersonDeserializer();

    @Test
    public void testDeserialize() {
        String json = "{\"id\":\"1\",\"name\":\"John Doe\"}";
        byte[] data = json.getBytes();

        // Deserialize the byte array
        var person = personDeserializer.deserialize("test-topic", data);

        // Assertions
        assert person != null;
        assert "1".equals(person.getId());
        assert "John Doe".equals(person.getName());

    }
}
