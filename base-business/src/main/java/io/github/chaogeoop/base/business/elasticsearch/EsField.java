package io.github.chaogeoop.base.business.elasticsearch;

import javax.lang.model.type.NullType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
//mapping使用的是直接定义的字段名（Field.geName()）,不要使用mongodb Field或者JsonProperty类注解，否则查询或反序列化结果会与预期不同。
public @interface EsField {
    EsTypeEnum type();

    boolean textHasKeywordField() default false;

    Class<?> objectType() default NullType.class;
}
