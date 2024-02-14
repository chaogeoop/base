package io.github.chaogeoop.base.business.common.annotations;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;
import io.github.chaogeoop.base.business.common.interfaces.IUserContextConverter;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

public class UserInfoAnnotationArgumentResolver implements HandlerMethodArgumentResolver {
    private final IUserContextConverter userContextConverter;

    public UserInfoAnnotationArgumentResolver(IUserContextConverter userContextConverter) {
        this.userContextConverter = userContextConverter;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        boolean existUserContext = BaseUserContext.class.isAssignableFrom(parameter.getParameterType());

        return existUserContext & parameter.hasParameterAnnotation(UserInfo.class);
    }

    @Override
    public BaseUserContext resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest request, WebDataBinderFactory binderFactory) {
        return this.userContextConverter.convert(request);
    }
}
