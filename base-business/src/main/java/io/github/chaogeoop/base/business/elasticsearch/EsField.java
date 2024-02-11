package io.github.chaogeoop.base.business.elasticsearch;

import javax.lang.model.type.NullType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EsField {
    EsTypeEnum type();

    boolean textHasKeywordField() default false;

    Class<?> objectType() default NullType.class;
}
