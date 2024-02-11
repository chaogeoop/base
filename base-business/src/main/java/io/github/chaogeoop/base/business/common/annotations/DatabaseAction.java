package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.common.enums.DatabaseActionEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseAction {
    DatabaseActionEnum action() default DatabaseActionEnum.PRIMARY_READ;

    int choosePrimarySeconds() default 10;
}
