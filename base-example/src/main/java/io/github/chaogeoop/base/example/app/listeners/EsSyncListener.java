package io.github.chaogeoop.base.example.app.listeners;

import io.github.chaogeoop.base.business.elasticsearch.EsProvider;
import io.github.chaogeoop.base.business.common.errors.DistributedLockedException;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.constants.RabbitmqConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@Slf4j
public class EsSyncListener {
    @Autowired
    private EsProvider esProvider;

    @RabbitListener(queues = RabbitmqConstants.ES_SYNC_QUEUE, admin = "taskAdmin", containerFactory = "taskRabbitListener")
    public void onMessage(Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String content = null;
        try {
            content = new String(message.getBody(), Charsets.UTF_8);
            List<EsProvider.EsNameId> list = JsonHelper.readValue(content, new TypeReference<>() {
            });
            if (list == null) {
                channel.basicAck(deliveryTag, false);
                return;
            }

            this.esProvider.syncToEs(list);

            channel.basicAck(deliveryTag, false);
        } catch (DistributedLockedException e) {
            channel.basicReject(deliveryTag, true);
        } catch (Exception e) {
            log.error("数据同步到es错误: {}, {}", content, e);
            channel.basicAck(deliveryTag, false);
        }
    }
}
