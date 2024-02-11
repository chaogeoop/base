package io.github.chaogeoop.base.business.mongodb.basic;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.data.mongodb.repository.support.QuerydslMongoPredicateExecutor;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import java.io.Serializable;

import static org.springframework.data.querydsl.QuerydslUtils.QUERY_DSL_PRESENT;

public class BaseMongoRepositoryFactoryBean<T extends MongoRepository<S, ID>, S, ID extends Serializable> extends MongoRepositoryFactoryBean<T, S, ID> {
    public BaseMongoRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Override
    protected RepositoryFactorySupport getFactoryInstance(MongoOperations operations) {
        return new BaseRepositoryFactory<>(operations);
    }

    private static class BaseRepositoryFactory<S, ID extends Serializable> extends MongoRepositoryFactory {
        public BaseRepositoryFactory(MongoOperations mongoOperations) {
            super(mongoOperations);
        }

        @Override
        protected RepositoryComposition.RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata, MongoOperations operations) {
            boolean isQueryDslRepository = QUERY_DSL_PRESENT
                    && QuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());

            if (isQueryDslRepository) {
                if (metadata.isReactiveRepository()) {
                    throw new InvalidDataAccessApiUsageException("error");
                }

                return RepositoryComposition.RepositoryFragments
                        .just(new QuerydslMongoPredicateExecutor<>(getEntityInformation(metadata.getDomainType()), operations));
            }

            return RepositoryComposition.RepositoryFragments.empty();
        }
    }
}
