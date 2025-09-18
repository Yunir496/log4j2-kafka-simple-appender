package com.example.log4j2kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.Throwables;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


@Plugin(name = "KafkaSimple", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class KafkaSimpleAppender extends AbstractAppender {

    private final String topic;
    private final Properties kafkaProps;
    private volatile KafkaProducer<String, byte[]> producer;

    private KafkaSimpleAppender(String name,
                                Filter filter,
                                Layout<? extends Serializable> layout,
                                boolean ignoreExceptions,
                                Property[] properties,
                                String topic,
                                Properties kafkaProps) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.topic = topic;
        this.kafkaProps = kafkaProps;
    }

    @PluginFactory
    public static KafkaSimpleAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginAttribute("topic") String topic,
            @PluginAttribute(value = "acks", defaultString = "all") String acks,
            @PluginAttribute(value = "lingerMs", defaultInt = 0) int lingerMs,
            @PluginAttribute(value = "retries", defaultInt = 3) int retries,
            @PluginAttribute(value = "compressionType", defaultString = "lz4") String compressionType,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") final Property[] extraProps
    ) {
        if (name == null) {
            LOGGER.error("KafkaSimpleAppender: 'name' is required");
            return null;
        }
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            LOGGER.error("KafkaSimpleAppender: 'bootstrapServers' is required");
            return null;
        }
        if (topic == null || topic.isBlank()) {
            LOGGER.error("KafkaSimpleAppender: 'topic' is required");
            return null;
        }

        if (layout == null) {
            layout = PatternLayout.newBuilder()
                    .withPattern("%d{ISO8601} %-5p [%t] %c - %m%n")
                    .build();
        }

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("linger.ms", Integer.toString(lingerMs));
        props.put("retries", Integer.toString(retries));
        props.put("compression.type", compressionType);
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());

        if (extraProps != null) {
            for (Property p : extraProps) {
                if (p != null && p.getName() != null && p.getValue() != null) {
                    if (!p.getName().equals("key.serializer") && !p.getName().equals("value.serializer")) {
                        props.put(p.getName(), p.getValue());
                    }
                }
            }
        }

        return new KafkaSimpleAppender(name, filter, layout, true, extraProps, topic, props);
    }

    @Override
    public void start() {
        super.start();
        try {
            this.producer = new KafkaProducer<>(kafkaProps);
        } catch (Exception e) {
            LOGGER.error("KafkaSimpleAppender: failed to start producer: {}", e.toString());
            Throwables.rethrow(e);
        }
    }

    @Override
    public void append(LogEvent event) {
        final KafkaProducer<String, byte[]> p = this.producer;
        if (p == null) {
            LOGGER.warn("KafkaSimpleAppender: producer not initialized yet");
            return;
        }
        try {
            byte[] payload;
            final Layout<? extends Serializable> l = getLayout();
            if (l != null) {
                payload = l.toByteArray(event);
            } else {
                final String s = event.getMessage() != null ? event.getMessage().getFormattedMessage() : "";
                payload = (s + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            }
            final String key = Objects.toString(event.getLoggerName(), null);
            p.send(new ProducerRecord<>(topic, key, payload));
        } catch (Exception e) {
            if (!ignoreExceptions()) {
                Throwables.rethrow(e);
            }
            LOGGER.error("KafkaSimpleAppender: failed to send log to topic '{}': {}", topic, e.toString());
        }
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        final KafkaProducer<String, byte[]> p = this.producer;
        this.producer = null;
        boolean stopped = super.stop(timeout, timeUnit);
        if (p != null) {
            try { p.flush(); } catch (Exception ignored) {}
            try { p.close(); } catch (Exception ignored) {}
        }
        return stopped;
    }
}
