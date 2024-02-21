package io.github.chaogeoop.base.business.redis;

import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistributedKeyProvider {
    private final MultiValueMap<Class<? extends KeyType>, KeyType> clazzTypes = new LinkedMultiValueMap<>();

    private final String scope;


    public DistributedKeyProvider(
            String registerPackageName,
            String scope
    ) {
        this.scope = scope;

        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(IKeyRegister.class));
        Set<BeanDefinition> components = provider.findCandidateComponents(registerPackageName);

        Set<KeyType> registeredTypes = new HashSet<>();

        for (BeanDefinition component : components) {
            try {
                Class<?> clazz = Class.forName(component.getBeanClassName());
                Object handler = clazz.getDeclaredConstructor().newInstance();

                Class<? extends KeyType> memberClazz = null;

                List<? extends KeyType> types = ((IKeyRegister<? extends KeyType>) handler).register();
                List<? extends KeyType> uniqueTypes = CollectionHelper.unique(types);
                if (types.size() != uniqueTypes.size()) {
                    throw new BizException(String.format("%s有重复类型", clazz.getName()));
                }

                for (KeyType type : types) {
                    if (memberClazz == null) {
                        memberClazz = type.getClass();

                        if (this.clazzTypes.containsKey(memberClazz)) {
                            throw new BizException(String.format("不能共用类型类: %s", memberClazz.getName()));
                        }
                    }

                    if (registeredTypes.contains(type)) {
                        throw new BizException(String.format("该锁类型已被其它模块注册: %s-%s", type.getType(), type.getSubType()));
                    }
                    registeredTypes.add(type);

                    this.clazzTypes.add(memberClazz, type);
                }

            } catch (Exception e) {
                throw new BizException(e);
            }
        }
    }

    public <T extends KeyType> String getKey(KeyEntity<T> keyEntity) {
        Class<? extends KeyType> clazz = keyEntity.getType().getClass();

        List<KeyType> types = this.clazzTypes.get(clazz);

        if (types == null || !types.contains(keyEntity.getType())) {
            throw new BizException(String.format("这个分布式key没有注册: %s-%s", keyEntity.getType().getType(), keyEntity.getType().getSubType()));
        }

        return String.format("%s:%s-%s:%s", this.scope, keyEntity.getType().getType(), keyEntity.getType().getSubType(), keyEntity.getTypeId());
    }
}
