package io.github.chaogeoop.base.business.mongodb.basic;

import org.springframework.data.mongodb.repository.config.MongoRepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

import java.lang.annotation.Annotation;

public class BaseMongoRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {
    @Override
    protected Class<? extends Annotation> getAnnotation() {
        return BaseEnableMongoRepositories.class;
    }

    @Override
    protected RepositoryConfigurationExtension getExtension() {
        return new MongoRepositoryConfigurationExtension();
    }
}
