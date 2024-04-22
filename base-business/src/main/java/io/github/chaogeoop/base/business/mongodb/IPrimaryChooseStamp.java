package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.interfaces.IUserContext;

import javax.annotation.Nullable;

public interface IPrimaryChooseStamp {
    void record(@Nullable IUserContext userContext, long stamp);

    Long read(@Nullable IUserContext userContext);
}
