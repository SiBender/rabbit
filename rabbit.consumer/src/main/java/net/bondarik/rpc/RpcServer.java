package net.bondarik.rpc;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Random;

public class RpcServer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);

        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");

        channel.basicConsume(RPC_QUEUE_NAME, false, getDeliverCallback(channel), (consumerTag -> {}));
    }

    private static DeliverCallback getDeliverCallback(Channel channel) {
        return (consumerTag, delivery) -> {
            String correlationId = delivery.getProperties().getCorrelationId();
            BasicProperties replyProps = new BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .build();

            String response = "";
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("message = '" + message + "'");
                response += String.format("[%s]Response for '%s' is %d", correlationId, message, random.nextInt());
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };


    }
}
