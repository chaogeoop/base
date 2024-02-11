package io.github.chaogeoop.base.business.mongodb.basic;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.repository.query.QueryLookupStrategy;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({BaseMongoRepositoriesRegistrar.class})
public @interface BaseEnableMongoRepositories {
    String[] value() default {};

    String[] basePackages() default {};

    Class<?>[] basePackageClasses() default {};

    ComponentScan.Filter[] includeFilters() default {};

    ComponentScan.Filter[] excludeFilters() default {};

    String repositoryImplementationPostfix() default "Impl";

    String namedQueriesLocation() default "";

    QueryLookupStrategy.Key queryLookupStrategy() default QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND;

    Class<?> repositoryFactoryBeanClass() default BaseMongoRepositoryFactoryBean.class;

    Class<?> repositoryBaseClass() default BaseSimpleMongoRepository.class;

    String mongoTemplateRef() default "mongoTemplate";

    boolean createIndexesForQueryMethods() default false;

    boolean considerNestedRepositories() default false;
}
