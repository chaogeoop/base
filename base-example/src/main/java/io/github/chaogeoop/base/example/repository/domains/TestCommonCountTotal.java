package io.github.chaogeoop.base.example.repository.domains;

import io.github.chaogeoop.base.business.common.CommonCountProvider;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("testcommoncounttotals")
public class TestCommonCountTotal extends CommonCountProvider.CommonCountTotal {
}
