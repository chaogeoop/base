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
    private final MultiValueMap<Class<? extends DistributedKeyType>, DistributedKeyType> clazzTypes = new LinkedMultiValueMap<>();

    private final String scope;


    public DistributedKeyProvider(
            String registerPackageName,
            String scope
    ) {
        this.scope = scope;

        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(IKeyRegister.class));
        Set<BeanDefinition> components = provider.findCandidateComponents(registerPackageName);

        Set<DistributedKeyType> registeredTypes = new HashSet<>();

        for (BeanDefinition component : components) {
            try {
                Class<?> clazz = Class.forName(component.getBeanClassName());
                Object handler = clazz.getDeclaredConstructor().newInstance();

                Class<? extends DistributedKeyType> memberClazz = null;

                List<? extends DistributedKeyType> types = ((IKeyRegister<? extends DistributedKeyType>) handler).register();
                List<? extends DistributedKeyType> uniqueTypes = CollectionHelper.unique(types);
                if (types.size() != uniqueTypes.size()) {
                    throw new BizException(String.format("%s有重复类型", clazz.getName()));
                }

                for (DistributedKeyType type : types) {
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

    public <T extends DistributedKeyType> String getKey(KeyEntity<T> keyEntity) {
        Class<? extends DistributedKeyType> clazz = keyEntity.getType().getClass();

        List<DistributedKeyType> types = this.clazzTypes.get(clazz);

        if (types == null || !types.contains(keyEntity.getType())) {
            throw new BizException(String.format("这个分布式key没有注册: %s-%s", keyEntity.getType().getType(), keyEntity.getType().getSubType()));
        }

        return String.format("%s:%s-%s:%s", this.scope, keyEntity.getType().getType(), keyEntity.getType().getSubType(), keyEntity.getTypeId());
    }

    @Setter
    @Getter
    public static class KeyEntity<T extends DistributedKeyType> {
        private T type;

        private String typeId;

        public static <M extends DistributedKeyType> KeyEntity<M> of(M type, String typeId
        ) {
            KeyEntity<M> data = new KeyEntity<>();

            data.setType(type);
            data.setTypeId(typeId);

            return data;
        }
    }
}
