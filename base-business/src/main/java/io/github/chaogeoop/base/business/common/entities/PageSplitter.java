package io.github.chaogeoop.base.business.common.entities;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class PageSplitter {
    private int offset;

    private int limit = 30;

    private String order;

    public PageSplitter() {

    }

    public PageSplitter(int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public PageSplitter(int offset, int limit, String order) {
        this.offset = offset;
        this.limit = limit;
        this.order = order;
    }

    public static PageSplitter of(int offset, int limit, String order) {
        return new PageSplitter(offset, limit, order);
    }

    public Sort calSort() {
        if (StringUtils.isBlank(this.order)) {
            return null;
        }

        List<Sort.Order> orders = Lists.newArrayList();
        for (String singleOrder : this.order.split(",")) {
            String orderItem = singleOrder.trim();

            Sort.Direction direction = Sort.Direction.ASC;
            String property = orderItem;
            if (orderItem.startsWith("-")) {
                direction = Sort.Direction.DESC;
                property = orderItem.substring(1).trim();
            } else if (orderItem.startsWith("+")) {
                property = orderItem.substring(1).trim();
            }

            orders.add(new Sort.Order(direction, property));
        }

        return Sort.by(orders);
    }

    public List<SortBuilder<?>> calEsSort() {
        List<SortBuilder<?>> orders = Lists.newArrayList();

        if (StringUtils.isBlank(this.order)) {
            return new ArrayList<>();
        }

        for (String singleOrder : this.order.split(",")) {
            String orderItem = singleOrder.trim();

            SortOrder direction = SortOrder.ASC;
            String property = orderItem;
            if (orderItem.startsWith("-")) {
                direction = SortOrder.DESC;
                property = orderItem.substring(1).trim();
            } else if (orderItem.startsWith("+")) {
                property = orderItem.substring(1).trim();
            }

            orders.add(SortBuilders.fieldSort(property).order(direction));
        }

        return orders;
    }
}
