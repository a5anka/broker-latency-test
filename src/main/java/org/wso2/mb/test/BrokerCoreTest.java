package org.wso2.mb.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.wso2.broker.common.BrokerConfigProvider;
import org.wso2.broker.common.StartupContext;
import org.wso2.broker.core.Broker;
import org.wso2.broker.core.configuration.BrokerConfiguration;

import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

public class BrokerCoreTest {
    public static void main(String[] args) throws Exception {
        MetricRegistry registry = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                                  .convertRatesTo(TimeUnit.SECONDS)
                                                  .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                  .build();
        reporter.start(20, TimeUnit.SECONDS);

        BrokerConfiguration brokerConfiguration = new BrokerConfiguration();
        TestConfigProvider configProvider = new TestConfigProvider();
        configProvider.registerConfigurationObject(BrokerConfiguration.NAMESPACE, brokerConfiguration);
        StartupContext startupContext = new StartupContext();
        startupContext.registerService(BrokerConfigProvider.class, configProvider);

        DbUtils.setupDB();
        DataSource dataSource = DbUtils.getDataSource();
        startupContext.registerService(DataSource.class, dataSource);

        Broker broker = new Broker(startupContext);
        broker.startMessageDelivery();

        Consumer consumer = new Consumer(broker, registry);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Publisher publisher = new Publisher(broker, 100000, 20000, registry, reporter);
        Thread publisherThread = new Thread(publisher);
        publisherThread.start();

        TimeUnit.SECONDS.sleep(30);
        consumerThread.join();
        publisherThread.join();
        broker.shutdown();
    }
}
