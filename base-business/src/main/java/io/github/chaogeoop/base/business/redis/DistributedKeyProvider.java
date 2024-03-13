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

import java.util.*;

public class DistributedKeyProvider {
    private final Map<Class<? extends KeyType>, Set<KeyType>> clazzTypes = new HashMap<>();

    private final Map<KeyFinder, KeyType> findTypeMap = new HashMap<>();

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

                List<? extends KeyType> types = ((IKeyRegister<? extends KeyType>) handler).register();
                if (types.isEmpty()) {
                    continue;
                }

                List<? extends KeyType> uniqueTypes = CollectionHelper.unique(types);
                if (types.size() != uniqueTypes.size()) {
                    throw new BizException(String.format("%s有重复类型", clazz.getName()));
                }

                Class<? extends KeyType> memberClazz = types.get(0).getClass();
                if (this.clazzTypes.containsKey(memberClazz)) {
                    throw new BizException(String.format("不能共用类型类: %s", memberClazz.getName()));
                }

                this.clazzTypes.put(memberClazz, new HashSet<>());

                for (KeyType type : types) {
                    if (registeredTypes.contains(type)) {
                        throw new BizException(String.format("该锁类型已被其它模块注册: %s-%s", type.getType(), type.getSubType()));
                    }
                    registeredTypes.add(type);

                    this.clazzTypes.get(memberClazz).add(type);
                    this.findTypeMap.put(KeyFinder.of(type), type);
                }

            } catch (Exception e) {
                throw new BizException(e);
            }
        }
    }

    public String getScope() {
        return this.scope;
    }

    public KeyType getKeyType(KeyFinder finder) {
        return this.findTypeMap.get(finder);
    }

    public <T extends KeyType> String getKey(KeyEntity<T> keyEntity) {
        Class<? extends KeyType> clazz = keyEntity.getType().getClass();

        Set<KeyType> types = this.clazzTypes.get(clazz);

        if (types == null || !types.contains(keyEntity.getType())) {
            throw new BizException(String.format("这个分布式key没有注册: %s-%s", keyEntity.getType().getType(), keyEntity.getType().getSubType()));
        }

        return String.format("%s:%s-%s:%s", this.scope, keyEntity.getType().getType(), keyEntity.getType().getSubType(), keyEntity.getTypeId());
    }

    @Setter
    @Getter
    public static class KeyFinder {
        private String type;

        private String subType;

        public static KeyFinder of(KeyType keyType) {
            KeyFinder data = new KeyFinder();

            data.setType(keyType.getType());
            data.setSubType(keyType.getSubType());

            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyFinder keyFinder = (KeyFinder) o;
            return Objects.equals(type, keyFinder.type) && Objects.equals(subType, keyFinder.subType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, subType);
        }
    }
}
