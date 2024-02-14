package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;
import io.github.chaogeoop.base.business.common.enums.DatabaseActionEnum;
import io.github.chaogeoop.base.business.mongodb.IPrimaryChooseStamp;
import io.github.chaogeoop.base.business.threadlocals.PrimaryChooseHolder;
import io.github.chaogeoop.base.business.common.errors.BizException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.Date;

@Aspect
@Order(2)
public class DatabaseActionAspect {
    private final IPrimaryChooseStamp handler;

    public DatabaseActionAspect(IPrimaryChooseStamp handler) {
        this.handler = handler;
    }


    @Pointcut("@annotation(io.github.chaogeoop.base.business.common.annotations.DatabaseAction)")
    public void pointCut() {
    }

    @Before("pointCut()")
    public void beforeHandler(JoinPoint jp) {
        Object[] args = jp.getArgs();

        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        Parameter[] parameters = method.getParameters();
        DatabaseAction annotation = method.getAnnotation(DatabaseAction.class);

        BaseUserContext userContext = null;

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];

            if (!parameter.isAnnotationPresent(UserInfo.class)) {
                continue;
            }

            if (!BaseUserContext.class.isAssignableFrom(parameter.getType())) {
                throw new BizException("only UserContext can have UserInfo annotation");
            }

            Object arg = args[i];
            if (arg == null) {
                throw new BizException("UserContext is null");
            }

            userContext = (BaseUserContext) arg;

            break;
        }

        if (userContext == null) {
            throw new BizException("cant find UserContext");
        }

        if (DatabaseActionEnum.JUDGE_READ.equals(annotation.action())) {
            Long stamp = this.handler.read(userContext);
            if (stamp == null) {
                stamp = new Date().getTime() - Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis();
            }

            PrimaryChooseHolder.init(stamp);
        }

        if (DatabaseActionEnum.SLAVER_READ.equals(annotation.action())) {
            PrimaryChooseHolder.init(new Date().getTime() - Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis());
        }
    }

    @After("pointCut()")
    public void afterHandler(JoinPoint jp) {
        Object[] args = jp.getArgs();

        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        Parameter[] parameters = method.getParameters();
        DatabaseAction annotation = method.getAnnotation(DatabaseAction.class);

        BaseUserContext userContext = null;

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];

            if (!parameter.isAnnotationPresent(UserInfo.class)) {
                continue;
            }

            if (!BaseUserContext.class.isAssignableFrom(parameter.getType())) {
                throw new BizException("only UserContext can have UserInfo annotation");
            }

            Object arg = args[i];
            if (arg == null) {
                throw new BizException("UseContext is null");
            }

            userContext = (BaseUserContext) arg;

            break;
        }

        if (userContext == null) {
            throw new BizException("cant find UserContext");
        }

        try {
            if (DatabaseActionEnum.WRITTEN_SOON_READ.equals(annotation.action())) {
                this.handler.record(userContext, new Date().getTime() + Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            PrimaryChooseHolder.remove();
        }
    }
}
