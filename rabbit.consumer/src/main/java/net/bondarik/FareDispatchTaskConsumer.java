package net.bondarik;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import net.bondarik.model.MessageWithDate;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

/**
 * Hello world!
 *
 */
public class FareDispatchTaskConsumer
{
    private final static String TASK_QUEUE_NAME = "tasks";
    private static final boolean isDurable = true;
    private static int counter = 0;

    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, isDurable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //enable Fare dispatching
        channel.basicQos(1);

        boolean autoAck = false; // acknowledgment is covered below
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, getDeliverCallback(channel), consumerTag -> { });
    }

    private static DeliverCallback getDeliverCallback(Channel channel) {
        return (consumerTag, delivery) -> {
            MessageWithDate message = deserialize(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");
            processTask(message);
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
    }

    private static void processTask(MessageWithDate taskMessage) {
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(taskMessage.getMessage().length() * 50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            long finish = System.currentTimeMillis();
            System.out.println("Message '" + taskMessage + "' processed in " + (finish - start) + " ms");
        }
    }

    private static MessageWithDate deserialize(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        try (ObjectInput in = new ObjectInputStream(bis)) {
            return (MessageWithDate) in.readObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
