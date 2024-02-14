package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.mongodb.basic.BaseEnableMongoRepositories;
import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

@Configuration
public class MongoMainConfiguration {
    @Value("${mongodb.main.connectUrl}")
    private String primaryUrl;

    @Primary
    @Bean("mongoMappingContext")
    public MongoMappingContext mongoMappingContext() {
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setAutoIndexCreation(true);
        return mappingContext;
    }

    @Primary
    @Bean("mappingMongoConverter")
    public MappingMongoConverter mappingMongoConverter(
            @Qualifier("mongoFactory") MongoDatabaseFactory factory, @Qualifier("mongoMappingContext") MongoMappingContext context
    ) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        return new MappingMongoConverter(dbRefResolver, context);
    }

    @Primary
    @Bean(name = "mongoTemplate")
    public MongoTemplate mongoTemplate(@Qualifier("mappingMongoConverter") MappingMongoConverter mongoConverter) throws Exception {
        MongoTemplate mongoTemplate = new MongoTemplate(dbFactory(), mongoConverter);
        mongoTemplate.setReadPreference(ReadPreference.primary());

        return mongoTemplate;
    }

    @Primary
    @Bean("mongoFactory")
    public MongoDatabaseFactory dbFactory() throws Exception {
        return new SimpleMongoClientDatabaseFactory(this.primaryUrl);
    }

    @Bean
    public MongoTransactionManager transactionManager() throws Exception {
        return new MongoTransactionManager(dbFactory());
    }

    @Bean("slaverMongoMappingContext")
    public MongoMappingContext slaverMongoMappingContext() {
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setAutoIndexCreation(false);
        return mappingContext;
    }

    @Bean("slaverMappingMongoConverter")
    public MappingMongoConverter slaverMappingMongoConverter(
            @Qualifier("slaverMongoFactory") MongoDatabaseFactory factory, @Qualifier("slaverMongoMappingContext") MongoMappingContext context
    ) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        return new MappingMongoConverter(dbRefResolver, context);
    }

    @Bean(name = "slaverMongoTemplate")
    public MongoTemplate slaverMongoTemplate(@Qualifier("slaverMappingMongoConverter") MappingMongoConverter mongoConverter) throws Exception {
        MongoTemplate mongoTemplate = new MongoTemplate(dbFactory(), mongoConverter);
        mongoTemplate.setReadPreference(ReadPreference.secondaryPreferred());

        return mongoTemplate;
    }

    @Bean("slaverMongoFactory")
    public MongoDatabaseFactory slaverDbFactory() {
        return new SimpleMongoClientDatabaseFactory(this.primaryUrl);
    }
}
