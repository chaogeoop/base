package io.github.chaogeoop.base.example.app.listeners;

import io.github.chaogeoop.base.business.common.IoMonitorProvider;
import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.common.errors.DistributedLockedException;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.constants.RabbitmqConstants;
import com.google.common.base.Charsets;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class IoMonitorListener {
    @Autowired
    private IoMonitorProvider ioMonitorProvider;

    @RabbitListener(queues = RabbitmqConstants.IO_MONITOR_QUEUE, admin = "taskAdmin", containerFactory = "taskRabbitListener")
    public void onMessage(Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String content = null;
        try {
            content = new String(message.getBody(), Charsets.UTF_8);
            IoStatistic newLog = JsonHelper.readValue(content, IoStatistic.class);

            this.ioMonitorProvider.persist(newLog, null);

            channel.basicAck(deliveryTag, false);
        } catch (DistributedLockedException e) {
            channel.basicReject(deliveryTag, true);
        } catch (Exception e) {
            log.error("io监控储存错误: {}, {}", content, e);
            channel.basicAck(deliveryTag, false);
        }
    }
}
