package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.entities.MongoPageSplitter;
import com.querydsl.core.types.Predicate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BaseSimpleMongoRepository<T extends BaseModel, ID extends Serializable> extends SimpleMongoRepository<T, ID> implements BaseMongoRepository<T, ID> {
    private final MongoOperations mongoOperations;

    private final MongoEntityInformation<T, ID> entityInformation;

    private final BaseSpringDataMongodbQuery<T> mongoQueryBuilder;

    public BaseSimpleMongoRepository(MongoEntityInformation<T, ID> metadata, MongoOperations mongoOperations) {
        super(metadata, mongoOperations);
        this.mongoOperations = mongoOperations;
        this.entityInformation = metadata;
        this.mongoQueryBuilder = BaseSpringDataMongodbQuery.of(this.mongoOperations, this.entityInformation.getJavaType());
    }

    @Override
    public T findFirst(Predicate predicate) {
        return this.findFirst(predicate, null);
    }

    @Override
    public T findFirst(Predicate predicate, Sort sort) {
        Query query = this.mongoQueryBuilder.buildQuery(predicate);

        return this.findFirst(query, sort);
    }

    @Override
    public T findFirst(Query query) {
        return this.findFirst(query, null);
    }

    @Override
    public T findFirst(Query query, Sort sort) {
        return MongoHelper.findFirst(this.mongoOperations, query, sort, this.entityInformation.getJavaType());
    }

    @Override
    public long count(Predicate predicate) {
        Query query = this.mongoQueryBuilder.buildQuery(predicate);

        return this.count(query);
    }

    @Override
    public long count(Query query) {
        return MongoHelper.count(this.mongoOperations, query, this.entityInformation.getJavaType());
    }

    @Override
    public ListPage<T> pageQuery(Predicate predicate, MongoPageSplitter mongoPageSplitter) {
        return this.pageQuery(predicate, new ArrayList<>(), mongoPageSplitter);
    }

    @Override
    public ListPage<T> pageQuery(Predicate predicate, List<String> fields, MongoPageSplitter mongoPageSplitter) {
        Query query = this.mongoQueryBuilder.buildQuery(predicate);

        return this.pageQuery(query, fields, mongoPageSplitter);
    }

    @Override
    public ListPage<T> pageQuery(Query query, MongoPageSplitter mongoPageSplitter) {
        return this.pageQuery(query, new ArrayList<>(), mongoPageSplitter);
    }

    @Override
    public ListPage<T> pageQuery(Query query, List<String> fields, MongoPageSplitter mongoPageSplitter) {
        return MongoHelper.pageQuery(this.mongoOperations, query, fields, mongoPageSplitter, this.entityInformation.getJavaType());
    }

    @Override
    public List<T> listQuery(Predicate predicate, Sort sort) {
        return this.listQuery(predicate, new ArrayList<>(), sort);
    }

    @Override
    public List<T> listQuery(Predicate predicate, List<String> fields, Sort sort) {
        Query query = this.mongoQueryBuilder.buildQuery(predicate);

        return this.listQuery(query, fields, sort);
    }

    @Override
    public List<T> listQuery(Query query, Sort sort) {
        return this.listQuery(query, new ArrayList<>(), sort);
    }

    @Override
    public List<T> listQuery(Query query, List<String> fields, Sort sort) {
        return MongoHelper.listQuery(this.mongoOperations, query, fields, sort, this.entityInformation.getJavaType());
    }
}
