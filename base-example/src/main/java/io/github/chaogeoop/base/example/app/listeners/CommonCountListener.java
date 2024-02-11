package io.github.chaogeoop.base.example.app.listeners;

import io.github.chaogeoop.base.business.common.CommonCountProvider;
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
public class CommonCountListener {
    @Autowired
    private CommonCountProvider commonCountProvider;

    @RabbitListener(queues = RabbitmqConstants.COMMON_COUNT_PERSIST_HISTORY_QUEUE, admin = "taskAdmin", containerFactory = "taskRabbitListener")
    public void onMessage(Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String content = null;
        try {
            content = new String(message.getBody(), Charsets.UTF_8);
            List<String> ids = JsonHelper.readValue(content, new TypeReference<>() {
            });
            if (ids == null) {
                channel.basicAck(deliveryTag, false);
                return;
            }

            this.commonCountProvider.historyToCount(ids);

            channel.basicAck(deliveryTag, false);
        } catch (DistributedLockedException e) {
            channel.basicReject(deliveryTag, true);
        } catch (Exception e) {
            log.error("计数持久化错误: {}, {}", content, e);
            channel.basicAck(deliveryTag, false);
        }
    }
}
