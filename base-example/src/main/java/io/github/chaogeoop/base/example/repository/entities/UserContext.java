package io.github.chaogeoop.base.example.repository.entities;

import io.github.chaogeoop.base.business.common.interfaces.IUserContext;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class UserContext implements IUserContext {
    private Long userId;
}
