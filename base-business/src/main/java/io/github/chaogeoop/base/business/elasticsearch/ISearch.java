package io.github.chaogeoop.base.business.elasticsearch;

import javax.annotation.Nullable;

public interface ISearch<M extends IBaseEs> {
    Class<M> giveEsModel();

    @Nullable
    M giveEsData();

    @Nullable
    default String giveEsJson() {
        M data = this.giveEsData();
        if (data == null) {
            return null;
        }

        return EsHelper.convertToJson(this.giveEsModel(), data);
    }
}
