package com.anassgassi.peoplestream.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.repository.config.EnableReactiveElasticsearchRepositories;

import java.time.Duration;

@Configuration
@EnableReactiveElasticsearchRepositories(basePackages = "com.anassgassi.peoplestream")
public class ReactiveElasticsearchConfig {

    @Bean
    public ReactiveElasticsearchClient reactiveElasticsearchClient(
            @Value("${spring.data.elasticsearch.client.reactive.endpoints}") String endpoint) {
        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(endpoint)
                .withConnectTimeout(Duration.ofSeconds(5))
                .withSocketTimeout(Duration.ofSeconds(3))
                .build();

        return ElasticsearchClients.createReactive(clientConfiguration);
    }

    @Bean
    public ReactiveElasticsearchOperations reactiveElasticsearchTemplate(
            ReactiveElasticsearchClient reactiveElasticsearchClient) {
        ElasticsearchConverter elasticsearchConverter = new MappingElasticsearchConverter(
                new SimpleElasticsearchMappingContext());

        return new ReactiveElasticsearchTemplate(reactiveElasticsearchClient, elasticsearchConverter);
    }
}