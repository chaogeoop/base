package io.github.chaogeoop.base.business.common.entities;

import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class EsPageSplitter extends PageSplitter {
    private List<SortBuilder<?>> esSort = new ArrayList<>();

    public EsPageSplitter(int offset, int limit) {
        super(offset, limit);
    }

    public EsPageSplitter(int offset, int limit, List<SortBuilder<?>> esSort) {
        super(offset, limit);
        this.esSort = esSort;
    }
}
