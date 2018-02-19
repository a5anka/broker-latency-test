package org.wso2.mb.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.wso2.broker.common.data.types.FieldTable;
import org.wso2.broker.common.data.types.FieldValue;
import org.wso2.broker.common.data.types.ShortString;
import org.wso2.broker.core.Broker;
import org.wso2.broker.core.BrokerException;
import org.wso2.broker.core.ContentChunk;
import org.wso2.broker.core.Message;
import org.wso2.broker.core.Metadata;

public class Publisher implements Runnable {
    private final Broker broker;
    private final int numberOfMessages;
    private final int publishRate;
    private final ConsoleReporter reporter;
    private final Meter meter;

    public Publisher(Broker broker, int numberOfMessages, int publishRate, MetricRegistry registry,
            ConsoleReporter reporter) {
        this.broker = broker;
        this.numberOfMessages = numberOfMessages;
        this.publishRate = publishRate;
        meter = registry.meter("message.publish.rate");
        this.reporter = reporter;
    }

    @Override
    public void run() {
        byte[] payload = "Test Message".getBytes();
        String routing = "MyTopic";
        String exchangeName = "amq.topic";
        ShortString startTime = ShortString.parseString("start");
        Message message = new Message(broker.getNextMessageId(), new Metadata(routing, exchangeName, payload.length));
        ByteBuf content = Unpooled.copiedBuffer(payload);
        message.addChunk(new ContentChunk(0, content));
        try {
            RateLimiter r = RateLimiter.create(publishRate);
            for (int i = 0; i < numberOfMessages; i++) {
                r.acquire();

                FieldTable fieldTable = new FieldTable();
                fieldTable.add(startTime, FieldValue.parseLongLongInt(System.currentTimeMillis()));
                Message newMessage = message.shallowCopyWith(broker.getNextMessageId(), routing, exchangeName);
                newMessage.getMetadata().setHeaders(fieldTable);
                broker.publish(newMessage);

                meter.mark();
            }
        } catch (BrokerException e) {
            e.printStackTrace();
        }
        reporter.report();
    }
}
