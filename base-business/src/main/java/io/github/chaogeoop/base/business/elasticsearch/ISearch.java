package io.github.chaogeoop.base.business.elasticsearch;

import javax.annotation.Nullable;

public interface ISearch<M extends BaseEs> {
    Class<M> giveEsModel();
    @Nullable
    M giveEsData();
}
