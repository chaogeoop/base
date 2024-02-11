package io.github.chaogeoop.base.business.elasticsearch;

import io.github.chaogeoop.base.business.common.errors.BizException;

import java.util.List;

public enum EsTypeEnum {
    LONG("long"),
    DOUBLE("double"),
    TEXT("text"),
    KEYWORD("keyword"),
    DATE("date"),
    BOOLEAN("boolean"),
    OBJECT("object"),
    NESTED("nested");

    public String getEsType() {
        return this.EsType;
    }

    private final String EsType;

    public static final List<EsTypeEnum> HAS_OBJECT_TYPES = List.of(EsTypeEnum.OBJECT, EsTypeEnum.NESTED);

    EsTypeEnum(String type) {
        this.EsType = type;
    }
}
