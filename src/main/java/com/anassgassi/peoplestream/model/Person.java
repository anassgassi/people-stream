package com.anassgassi.peoplestream.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Setter
@Getter
@Document(indexName = "persons")
public class Person {

    @Id
    private String id;

    @NotBlank(message = "Name must not be blank")
    private String name;

    @NotNull(message = "Age must not be null")
    @Min(value = 0, message = "Age must be greater than or equal to 0")
    private Integer age;

}