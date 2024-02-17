package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.elasticsearch.IBaseEs;
import io.github.chaogeoop.base.business.elasticsearch.EsProvider;
import io.github.chaogeoop.base.business.elasticsearch.ISearch;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.IPrimaryChoose;
import io.github.chaogeoop.base.business.mongodb.InitCollectionIndexHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SpringBootApplication(scanBasePackages = "io.github.chaogeoop.base.example")
public class AppApplication {
    @Autowired
    private List<IPrimaryChoose<? extends BaseModel>> daoList;

    @Autowired
    private List<EsProvider> esProviders;

    @Bean
    public void databaseInit() {
        Set<Class<? extends ISearch<? extends IBaseEs>>> searchClazzList = new HashSet<>();

        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(ISearch.class));
        Set<BeanDefinition> components = provider.findCandidateComponents("io.github.chaogeoop.base.example.repository.domains");

        for (BeanDefinition son : components) {
            try {
                Class<?> aClass = Class.forName(son.getBeanClassName());
                searchClazzList.add((Class<? extends ISearch<? extends IBaseEs>>) aClass);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        new InitCollectionIndexHandler(this.daoList, searchClazzList, esProviders);
    }


    public static void main(String[] args) {
        SpringApplication.run(AppApplication.class, args);
    }
}
