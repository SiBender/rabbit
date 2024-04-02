package net.bondarik.rpc;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RpcClient {
    private static final String RPC_QUEUE_NAME = "rpc_queue";
    private static final Scanner SCANNER = new Scanner(System.in);

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            while (true) {
                System.out.println(call(channel, SCANNER.nextLine()));
            }
        }

    }

    private static String call(Channel channel, String message) throws IOException, ExecutionException, InterruptedException {
        final String correlationId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        BasicProperties props = new BasicProperties.Builder()
                                                   .correlationId(correlationId)
                                                   .replyTo(replyQueueName)
                                                   .build();

        channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));

        final CompletableFuture<String> response = new CompletableFuture<>();

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                response.complete(new String(delivery.getBody(), "UTF-8"));
            }
        }
        , consumerTag -> {}
        );

        String result = response.get();
        channel.basicCancel(ctag);
        return result;
    }
}
