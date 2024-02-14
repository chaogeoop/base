package io.github.chaogeoop.base.example.repository.entities;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class UserContext extends BaseUserContext {
    private Long userId;
}
