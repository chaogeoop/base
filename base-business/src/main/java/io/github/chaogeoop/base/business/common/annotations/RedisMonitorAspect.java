package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.threadlocals.IoMonitorHolder;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;

@Aspect
public class RedisMonitorAspect {
    @Pointcut("execution(* io.github.chaogeoop.base.business.redis.RedisProvider.*(..))")
    public void redisMonitorHandle() {
    }

    @Before("redisMonitorHandle()")
    public void beforeMethod(JoinPoint jp) {
        Method method = ((MethodSignature) jp.getSignature()).getMethod();

        IoMonitorHolder.incRedis(method.getName());
    }
}
