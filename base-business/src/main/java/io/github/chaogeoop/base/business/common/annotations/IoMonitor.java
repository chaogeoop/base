package io.github.chaogeoop.base.business.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IoMonitor {
    int databaseLimit() default 0;

    int countLimit() default 0;

    int redisLimit() default 0;

    int intervalTimes() default 1000;

    int devIntervalTimes() default 1;
}
