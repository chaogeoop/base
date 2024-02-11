package io.github.chaogeoop.base.business.common.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class ListPage<T> {
    private Integer offset;

    private Integer limit;

    private long count;

    private List<T> list = new ArrayList<>();


    public static <T> ListPage<T> of(Integer offset, Integer limit, long count, List<T> list) {
        ListPage<T> page = new ListPage<>();

        page.setOffset(offset);
        page.setLimit(limit);
        page.setCount(count);
        page.setList(list);

        return page;
    }
}
