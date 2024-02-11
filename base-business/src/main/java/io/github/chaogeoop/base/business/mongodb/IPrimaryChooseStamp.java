package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.entities.UserContext;

public interface IPrimaryChooseStamp {
    void record(UserContext userContext, long stamp);

    Long read(UserContext userContext);
}
