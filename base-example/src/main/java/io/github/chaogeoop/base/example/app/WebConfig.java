package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.common.GlobalExceptionHandler;
import io.github.chaogeoop.base.business.common.annotations.UserInfoAnnotationArgumentResolver;
import io.github.chaogeoop.base.example.app.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    @Autowired
    private UserService userService;

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new UserInfoAnnotationArgumentResolver(this.userService));
    }

    @RestControllerAdvice
    public static class AppGlobalExceptionHandler extends GlobalExceptionHandler {

    }




}
