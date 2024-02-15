package io.github.chaogeoop.base.business.common.entities;

import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class MongoPageSplitter extends PageSplitter {
    private Sort mongoSort;

    public MongoPageSplitter(int offset, int limit) {
        super(offset, limit);
    }

    public MongoPageSplitter(int offset, int limit, Sort mongoSort) {
        super(offset, limit);
        this.mongoSort = mongoSort;
    }
}
