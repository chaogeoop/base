package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.mongodb.SplitCollectionHelper;
import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
import io.github.chaogeoop.base.business.threadlocals.IoMonitorHolder;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

@Aspect
public class MongoMonitorAspect {
    private final static List<String> COUNT_FUNC_LIST = List.of("count", "estimatedCount");

    @Pointcut("execution(* org.springframework.data.mongodb.core.MongoTemplate.findOne(..))")
    public void mongoFindOneMonitorHandle() {
    }

    @Pointcut("execution(* org.springframework.data.mongodb.core.MongoTemplate.find(..))")
    public void mongoFindMultiMonitorHandle() {
    }

    @Pointcut("execution(* org.springframework.data.mongodb.core.MongoTemplate.count(..))")
    public void mongoCountMonitorHandle() {
    }

    @Pointcut("execution(* org.springframework.data.mongodb.core.MongoTemplate.estimatedCount(..))")
    public void mongoEstimatedCountMonitorHandle() {
    }

    @Before("mongoFindOneMonitorHandle() || mongoFindMultiMonitorHandle() || mongoCountMonitorHandle() || mongoEstimatedCountMonitorHandle()")
    public void beforeHandle(JoinPoint jp) {
        Object[] args = jp.getArgs();

        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        Parameter[] parameters = method.getParameters();

        String targetCollectionName = getTargetCollectionName(parameters, args);

        if (StringUtils.isBlank(targetCollectionName)) {
            return;
        }

        if (COUNT_FUNC_LIST.contains(method.getName())) {
            IoMonitorHolder.incCount(targetCollectionName);
        } else {
            IoMonitorHolder.incDatabase(targetCollectionName);
        }
    }

    private String getTargetCollectionName(Parameter[] parameters, Object[] args) {
        String collectionName = "";
        String baseCollectionName = "";
        Class<? extends BaseModel> clazz = null;

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            Object arg = args[i];

            if (arg instanceof Class && BaseModel.class.isAssignableFrom((Class<?>) arg)) {
                try {
                    baseCollectionName = BaseModel.getBaseCollectionNameByClazz((Class<? extends BaseModel>) arg);
                    clazz = (Class<? extends BaseModel>) arg;
                } catch (Exception ignored) {

                }
            }

            if (!String.class.equals(parameter.getType()) || !(arg instanceof String)) {
                continue;
            }

            collectionName = (String) arg;
        }


        if (!StringUtils.isBlank(collectionName)) {
            if (clazz == null) {
                return collectionName;
            }

            if (SplitCollectionHelper.isClazzRelativeCollection(collectionName, clazz)) {
                return baseCollectionName;
            }

            return collectionName;
        }

        return baseCollectionName;
    }
}
