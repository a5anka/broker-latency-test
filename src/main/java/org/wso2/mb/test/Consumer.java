package org.wso2.mb.test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.wso2.broker.common.ValidationException;
import org.wso2.broker.common.data.types.FieldTable;
import org.wso2.broker.common.data.types.LongLongInt;
import org.wso2.broker.common.data.types.ShortString;
import org.wso2.broker.core.Broker;
import org.wso2.broker.core.BrokerException;
import org.wso2.broker.core.Message;

public class Consumer implements Runnable {
    private final Broker broker;
    private final Histogram latencyHistogram;

    public Consumer(Broker broker, MetricRegistry registry) {
        this.broker = broker;
        latencyHistogram = registry.histogram("message.latency");
    }

    @Override
    public void run() {
        try {
            String queueName = String.valueOf(System.currentTimeMillis());
            broker.createQueue(queueName, false, false, true);
            broker.bind(queueName, "amq.topic", "MyTopic", FieldTable.EMPTY_TABLE);
            MessageConsumer consumer = new MessageConsumer(queueName, latencyHistogram);
            broker.addConsumer(consumer);
        } catch (BrokerException | ValidationException e) {
            e.printStackTrace();
        }

    }

    private static class MessageConsumer extends org.wso2.broker.core.Consumer {

        private final String queueName;
        private final Histogram latencyHistogram;
        private final ShortString startTimeHeadername;

        public MessageConsumer(String queueName, Histogram latencyHistogram) {
            this.queueName = queueName;
            this.latencyHistogram = latencyHistogram;
            startTimeHeadername = ShortString.parseString("start");
        }

        @Override
        protected void send(Message message) throws BrokerException {
            LongLongInt value = (LongLongInt) message.getMetadata().getHeaders().getValue(startTimeHeadername).getValue();
            long startTime = value.getLong();
            long endTime = System.currentTimeMillis();
            latencyHistogram.update(endTime - startTime);
        }

        @Override
        public String getQueueName() {
            return queueName;
        }

        @Override
        protected void close() throws BrokerException {

        }

        @Override
        public boolean isExclusive() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }
}
