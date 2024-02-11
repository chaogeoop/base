package io.github.chaogeoop.base.example.repository.domains;

import io.github.chaogeoop.base.business.elasticsearch.EsProvider;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Document("essynclogs")
public class EsSyncLog extends EsProvider.SyncLog {
}
