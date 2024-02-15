package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.interfaces.IUserContext;

public interface IPrimaryChooseStamp {
    void record(IUserContext userContext, long stamp);

    Long read(IUserContext userContext);
}
