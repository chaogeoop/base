package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.StrictRedisProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfiguration {
    @Value("${redis.host}")
    private String hostname;

    @Value("${redis.port}")
    private int port;

    @Value("${redis.password}")
    private String password;


    @Bean
    DistributedKeyProvider distributedKeyProvider() {
        return new DistributedKeyProvider("io.github.chaogeoop.base.example.app.keyregisters", "app");
    }

    @Bean
    public JedisConnectionFactory jedisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(hostname);
        config.setPort(port);
        config.setPassword(password);
        return new JedisConnectionFactory(config);
    }

    @Bean
    public <T> RedisTemplate<String, T> redisTemplate(JedisConnectionFactory jedisConnectionFactory) {
        RedisTemplate<String, T> template = new RedisTemplate<>();
        template.setConnectionFactory(jedisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        Jackson2JsonRedisSerializer<?> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);

        template.setValueSerializer(jackson2JsonRedisSerializer);
        template.setHashValueSerializer(jackson2JsonRedisSerializer);
        return template;
    }

    @Bean
    public StrictRedisProvider redisProvider(RedisTemplate<String, Object> redisTemplate, DistributedKeyProvider distributedKeyProvider) {
        return new StrictRedisProvider(redisTemplate, distributedKeyProvider);
    }
}
