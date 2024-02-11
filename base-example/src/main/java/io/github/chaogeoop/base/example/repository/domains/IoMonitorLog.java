package io.github.chaogeoop.base.example.repository.domains;

import io.github.chaogeoop.base.business.common.IoMonitorProvider;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;


@Setter
@Getter
@Document("iomonitorlog")
public class IoMonitorLog extends IoMonitorProvider.MonitorLog {
}
