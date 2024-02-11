package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.types.Predicate;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.support.SpringDataMongodbQuery;

import java.util.ArrayList;
import java.util.Map;

public class BaseSpringDataMongodbQuery<T> extends SpringDataMongodbQuery<T> {
    private final QueryModifiers EMPTY_MODIFIERS = new QueryModifiers(null, null);

    public BaseSpringDataMongodbQuery(MongoOperations operations, Class<? extends T> type) {
        super(operations, type);
    }

    public Query buildQuery(Predicate predicate) {
        return this.createQuery(predicate, null, EMPTY_MODIFIERS, new ArrayList<>());
    }

    public Query buildQuery(Document document) {
        return new BasicQuery(document);
    }

    public Query buildQuery(Map<String, Object> map) {
        return new BasicQuery(Document.parse(JsonHelper.writeValueAsString(map)));
    }

    public static <T> BaseSpringDataMongodbQuery<T> of(MongoOperations operations, Class<? extends T> type) {
        return new BaseSpringDataMongodbQuery<>(operations, type);
    }
}
