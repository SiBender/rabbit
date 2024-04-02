package net.bondarik.rabbit.config;

import net.bondarik.rabbit.MessageProducer;
import net.bondarik.rabbit.MessageConsumer;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class ProducerConfiguration {
    @Bean
    public Queue hello() {
        return new Queue("hello");
    }

    @Profile("receiver")
    @Bean
    public MessageConsumer consumer() {
        return new MessageConsumer();
    }

    @Profile("sender")
    @Bean
    public MessageProducer producer() {
        return new MessageProducer();
    }
}
