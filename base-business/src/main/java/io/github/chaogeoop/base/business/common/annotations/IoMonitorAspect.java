package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.common.interfaces.IoMonitorPersist;
import io.github.chaogeoop.base.business.threadlocals.IoMonitorHolder;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Random;

@Aspect
@Order(1)
public class IoMonitorAspect {
    private static final Random rand = new Random();

    private final IoMonitorPersist handler;

    private final boolean isDev;

    public IoMonitorAspect(IoMonitorPersist handler, boolean isDev) {
        this.handler = handler;
        this.isDev = isDev;
    }

    @Pointcut("@annotation(io.github.chaogeoop.base.business.common.annotations.IoMonitor)")
    public void ioMonitorHandle() {
    }

    @Before("ioMonitorHandle()")
    public void beforeHandler(JoinPoint jp) {
        Method method = ((MethodSignature) jp.getSignature()).getMethod();


        IoMonitorHolder.init(method.getDeclaringClass().getName() + "." + method.getName());
    }

    @After("ioMonitorHandle()")
    public void afterHandler(JoinPoint jp) {
        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        IoMonitor annotation = method.getAnnotation(IoMonitor.class);

        try {
            String funcName = method.getDeclaringClass().getName() + "." + method.getName();
            IoStatistic ioStatistic = IoMonitorHolder.get(funcName);
            IoMonitorHolder.removeFunc(funcName);

            int randomValue;
            if (this.isDev) {
                randomValue = rand.nextInt(annotation.devIntervalTimes());
            } else {
                randomValue = rand.nextInt(annotation.intervalTimes());
            }

            if (ioStatistic != null && randomValue == 0) {
                long databaseTime = 0;
                long countTime = 0;
                long redisTime = 0;

                for (Map.Entry<String, Long> entry : ioStatistic.getDatabase().entrySet()) {
                    databaseTime += entry.getValue();
                }

                for (Map.Entry<String, Long> entry : ioStatistic.getCount().entrySet()) {
                    countTime += entry.getValue();
                }

                for (Map.Entry<String, Long> entry : ioStatistic.getRedis().entrySet()) {
                    redisTime += entry.getValue();
                }

                if (databaseTime > annotation.databaseLimit() || redisTime > annotation.redisLimit() || countTime > annotation.countLimit()) {
                    this.handler.handle(ioStatistic);
                }
            }
        } catch (Exception ignored) {

        } finally {
            IoMonitorHolder.remove();
        }
    }
}
