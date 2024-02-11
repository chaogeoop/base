package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.common.errors.BizException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.math.BigInteger;

@Setter
@Getter
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class RootModel {
    @Id
    BigInteger id;

    @Version
    @Field(value = "__v")
    private Long v;

    public static String getBaseCollectionNameByClazz(Class<? extends RootModel> clazz) {
        if (!clazz.isAnnotationPresent(Document.class)) {
            throw new BizException("找不到表名");
        }

        return clazz.getAnnotation(Document.class).value();
    }

    public static String getNameInIdCollectionByClazz(Class<? extends RootModel> clazz) {
        if (clazz.isAnnotationPresent(NameInIdCollection.class)) {
            return clazz.getAnnotation(NameInIdCollection.class).name();
        }

        return getBaseCollectionNameByClazz(clazz);
    }
}
