package io.github.chaogeoop.base.example.app;

import com.google.common.collect.Lists;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfiguration {
    @Value("${elasticsearch.jest.uris}")
    private String jestUris;

    @Bean
    public JestClient getJestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(Lists.newArrayList(jestUris.split(",")))
                .connTimeout(3000)
                .readTimeout(3000)
                .multiThreaded(true)
                .build()
        );
        return factory.getObject();
    }
}
