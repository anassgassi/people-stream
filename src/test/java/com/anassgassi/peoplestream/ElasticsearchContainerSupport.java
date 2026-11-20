package com.anassgassi.peoplestream;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts one Elasticsearch container shared by all tests that extend this class.
 */
public abstract class ElasticsearchContainerSupport {

    static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.13.4"))
                    .withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    static {
        ELASTICSEARCH.start();
    }

    @DynamicPropertySource
    static void elasticsearchProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.elasticsearch.uris", ELASTICSEARCH::getHttpHostAddress);
        registry.add("spring.data.elasticsearch.client.reactive.endpoints", ELASTICSEARCH::getHttpHostAddress);
    }
}

