package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;

public interface IPrimaryChooseStamp {
    void record(BaseUserContext userContext, long stamp);

    Long read(BaseUserContext userContext);
}
