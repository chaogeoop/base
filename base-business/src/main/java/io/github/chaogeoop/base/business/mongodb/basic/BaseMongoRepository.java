package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.entities.MongoPageSplitter;
import com.querydsl.core.types.Predicate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.io.Serializable;
import java.util.List;

public interface BaseMongoRepository<T, ID extends Serializable> extends MongoRepository<T, ID> {
    T findFirst(Predicate predicate);

    T findFirst(Predicate predicate, Sort sort);

    T findFirst(Query query);

    T findFirst(Query query, Sort sort);

    long count(Predicate predicate);

    long count(Query query);

    ListPage<T> pageQuery(Predicate predicate, MongoPageSplitter mongoPageSplitter);

    ListPage<T> pageQuery(Predicate predicate, List<String> fields, MongoPageSplitter mongoPageSplitter);

    ListPage<T> pageQuery(Query query, MongoPageSplitter mongoPageSplitter);

    ListPage<T> pageQuery(Query query, List<String> fields, MongoPageSplitter mongoPageSplitter);

    List<T> listQuery(Predicate predicate, Sort sort);

    List<T> listQuery(Predicate predicate, List<String> fields, Sort sort);

    List<T> listQuery(Query query, Sort sort);

    List<T> listQuery(Query query, List<String> fields, Sort sort);
}
