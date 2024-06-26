package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.common.interfaces.IUserContext;
import io.github.chaogeoop.base.business.common.enums.DatabaseActionEnum;
import io.github.chaogeoop.base.business.mongodb.IPrimaryChooseStamp;
import io.github.chaogeoop.base.business.threadlocals.PrimaryChooseHolder;
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
        String funcName = method.getDeclaringClass().getName() + "." + method.getName();

        if (DatabaseActionEnum.JUDGE_READ.equals(annotation.action())) {
            IUserContext userContext = getUserContext(parameters, args);
            Long stamp = this.handler.read(userContext);
            if (stamp == null) {
                stamp = new Date().getTime() - Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis();
            }

            PrimaryChooseHolder.init(stamp, funcName);
            return;
        }

        if (DatabaseActionEnum.SLAVER_READ.equals(annotation.action())) {
            PrimaryChooseHolder.init(new Date().getTime() - Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis(), funcName);
            return;
        }

        PrimaryChooseHolder.init(new Date().getTime() + Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis(), funcName);
    }

    @After("pointCut()")
    public void afterHandler(JoinPoint jp) {
        Object[] args = jp.getArgs();

        Method method = ((MethodSignature) jp.getSignature()).getMethod();
        Parameter[] parameters = method.getParameters();
        DatabaseAction annotation = method.getAnnotation(DatabaseAction.class);
        String funcName = method.getDeclaringClass().getName() + "." + method.getName();

        try {
            if (DatabaseActionEnum.WRITTEN_SOON_READ.equals(annotation.action())) {
                IUserContext userContext = getUserContext(parameters, args);
                this.handler.record(userContext, new Date().getTime() + Duration.ofSeconds(annotation.choosePrimarySeconds()).toMillis());
            }
        } catch (Exception ignored) {

        } finally {
            PrimaryChooseHolder.remove(funcName);
        }
    }

    private static IUserContext getUserContext(Parameter[] parameters, Object[] args) {
        IUserContext userContext = null;

        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];

            if (!IUserContext.class.isAssignableFrom(parameter.getType())) {
                continue;
            }

            Object arg = args[i];
            if (arg == null) {
                continue;
            }

            userContext = (IUserContext) arg;

            break;
        }

        return userContext;
    }
}
