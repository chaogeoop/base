package io.github.chaogeoop.base.business.common.entities;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class PageSplitter {
    private int offset;

    private int limit;

    PageSplitter(int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
    }
}
