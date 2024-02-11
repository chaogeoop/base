package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.example.app.services.UserService;
import io.github.chaogeoop.base.business.common.annotations.DatabaseActionAspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AspectConfig {
    @Autowired
    private UserService userService;

    @Bean
    public DatabaseActionAspect databaseActionAspect() {
        return new DatabaseActionAspect(this.userService);
    }
}
