package com.example.pipeline;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpServer;

public class FeaturePipelineApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeaturePipelineApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
    private static final PipelineMetrics METRICS = new PipelineMetrics();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("kafka.bootstrap.servers", "kafka:9092");
        String userEventsTopic = env("user.events.topic", "user-events");
        String contentMetadataTopic = env("content.metadata.topic", "content-metadata");
        String featureStoreTopic = env("feature.store.topic", "feature-store");
        String metricsTopic = env("pipeline.metrics.topic", "pipeline-metrics");
        int httpPort = Integer.parseInt(env("pipeline.http.port", "8090"));
        int parallelism = Integer.parseInt(env("pipeline.parallelism", "1"));
        int startupMaxAttempts = Integer.parseInt(env("pipeline.kafka.startup.max.attempts", "30"));
        int startupRetrySeconds = Integer.parseInt(env("pipeline.kafka.startup.retry.seconds", "2"));
        int userWindowMinutes = Integer.parseInt(env("pipeline.user.window.minutes", "5"));
        int contentWindowMinutes = Integer.parseInt(env("pipeline.content.window.minutes", "5"));
        int contentSlideMinutes = Integer.parseInt(env("pipeline.content.slide.minutes", "1"));
        int categoryWindowMinutes = Integer.parseInt(env("pipeline.category.window.minutes", "5"));
        Path readyFile = Path.of("/tmp/feature-pipeline.ready");

        LOGGER.info("Starting feature pipeline with bootstrap={}, userTopic={}, metadataTopic={}, featureTopic={}, metricsTopic={}",
            bootstrapServers,
            userEventsTopic,
            contentMetadataTopic,
            featureStoreTopic,
            metricsTopic);

        waitForKafkaAndEnsureTopics(
            bootstrapServers,
            Map.of(
                userEventsTopic, new TopicSpec(3, (short) 1, Map.of()),
                contentMetadataTopic, new TopicSpec(1, (short) 1, Map.of("cleanup.policy", "compact")),
                featureStoreTopic, new TopicSpec(1, (short) 1, Map.of("cleanup.policy", "compact")),
                metricsTopic, new TopicSpec(1, (short) 1, Map.of())),
            startupMaxAttempts,
            startupRetrySeconds);

        startHealthServer(httpPort, readyFile);
        startMetricsPublisher(bootstrapServers, metricsTopic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(30_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, org.apache.flink.api.common.time.Time.seconds(5)));

        KafkaSource<String> userSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(userEventsTopic)
                .setGroupId("feature-pipeline-user-events")
            .setProperty("request.timeout.ms", "30000")
            .setProperty("session.timeout.ms", "45000")
            .setProperty("retry.backoff.ms", "1000")
            .setProperty("reconnect.backoff.ms", "1000")
            .setProperty("reconnect.backoff.max.ms", "10000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        KafkaSource<String> metadataSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(contentMetadataTopic)
                .setGroupId("feature-pipeline-content-metadata")
                .setProperty("request.timeout.ms", "30000")
                .setProperty("session.timeout.ms", "45000")
                .setProperty("retry.backoff.ms", "1000")
                .setProperty("reconnect.backoff.ms", "1000")
                .setProperty("reconnect.backoff.max.ms", "10000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        DataStream<UserEvent> userEvents = env.fromSource(userSource, WatermarkStrategy.noWatermarks(), "user-events-source")
                .map(FeaturePipelineApp::parseUserEventSafe)
                .returns(UserEvent.class)
                .filter(Objects::nonNull)
                .map(event -> {
                    METRICS.observeEventTime(event.eventTimeMillis());
                    METRICS.incrementConsumedEvents();
                    return event;
                })
                .returns(UserEvent.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (event, recordTimestamp) -> event.eventTimeMillis()))
                .name("user-events-watermarks");

        DataStream<ContentMetadata> metadataEvents = env.fromSource(metadataSource, WatermarkStrategy.noWatermarks(), "content-metadata-source")
                .map(FeaturePipelineApp::parseContentMetadataSafe)
                .returns(ContentMetadata.class)
                .filter(Objects::nonNull)
                .map(m -> {
                    METRICS.incrementConsumedEvents();
                    return m;
                })
                .returns(ContentMetadata.class)
                .name("content-metadata-parse");

        MapStateDescriptor<String, ContentMetadata> metadataStateDescriptor = new MapStateDescriptor<>(
                "content-metadata-state",
                org.apache.flink.api.common.typeinfo.Types.STRING,
                TypeInformation.of(ContentMetadata.class));

        BroadcastStream<ContentMetadata> broadcastMetadata = metadataEvents.broadcast(metadataStateDescriptor);

        DataStream<EnrichedEvent> enrichedEvents = userEvents.connect(broadcastMetadata)
                .process(new MetadataEnrichmentFunction(metadataStateDescriptor))
                .name("metadata-enrichment");

        OutputTag<UserEvent> lateUserEventsTag = new OutputTag<>("late-user-events") {};

        SingleOutputStreamOperator<FeatureRecord> userFeatures = userEvents
                .keyBy(UserEvent::userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(userWindowMinutes)))
                .sideOutputLateData(lateUserEventsTag)
                .aggregate(new UserFeatureAggregate(), new UserFeatureWindowFunction())
                .name("user-feature-window");

        userFeatures.getSideOutput(lateUserEventsTag)
                .map(event -> {
                    METRICS.incrementLateEventsDropped();
                    return event;
                })
                .returns(UserEvent.class)
                .addSink(new DiscardingSink<>())
                .name("late-user-event-discard");

        SingleOutputStreamOperator<FeatureRecord> contentFeatures = userEvents
                .keyBy(UserEvent::contentId)
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(contentWindowMinutes), Duration.ofMinutes(contentSlideMinutes)))
                .aggregate(new ContentFeatureAggregate(), new ContentFeatureWindowFunction())
                .name("content-feature-window");

        SingleOutputStreamOperator<FeatureRecord> categoryFeatures = enrichedEvents
                .keyBy(EnrichedEvent::userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(categoryWindowMinutes)))
                .aggregate(new CategoryAffinityAggregate(), new CategoryAffinityWindowFunction())
                .name("category-affinity-window");

        userFeatures
            .map(FeaturePipelineApp::trackProducedFeature)
            .returns(FeatureRecord.class)
            .sinkTo(createFeatureSink(featureStoreTopic, bootstrapServers))
            .name("user-feature-sink");
        contentFeatures
            .map(FeaturePipelineApp::trackProducedFeature)
            .returns(FeatureRecord.class)
            .sinkTo(createFeatureSink(featureStoreTopic, bootstrapServers))
            .name("content-feature-sink");
        categoryFeatures
            .map(FeaturePipelineApp::trackProducedFeature)
            .returns(FeatureRecord.class)
            .sinkTo(createFeatureSink(featureStoreTopic, bootstrapServers))
            .name("category-feature-sink");

        Files.writeString(readyFile, "ready");
        LOGGER.info("Pipeline initialized successfully. Executing Flink job.");
        env.execute("real-time-feature-engineering-pipeline");
    }

        private static FeatureRecord trackProducedFeature(FeatureRecord record) {
        METRICS.incrementProducedFeatures();
        return record;
        }

    private static KafkaSink<FeatureRecord> createFeatureSink(String topic, String bootstrapServers) {
        return KafkaSink.<FeatureRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FeatureRecord>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new FeatureRecordKeySerializer())
                        .setValueSerializationSchema(new FeatureRecordValueSerializer())
                        .build())
                .build();
    }

    private static void startHealthServer(int port, Path readyFile) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        server.createContext("/health", exchange -> {
            try {
                String body = MAPPER.writeValueAsString(Map.of(
                        "ready", Files.exists(readyFile),
                        "pid", ProcessHandle.current().pid(),
                        "consumed_events", METRICS.consumedEvents.get(),
                        "produced_features", METRICS.producedFeatures.get(),
                        "parse_errors", METRICS.parseErrors.get(),
                        "late_events_dropped", METRICS.lateEventsDropped.get(),
                        "watermark_lag_ms", METRICS.watermarkLagMillis.get()));
                byte[] response = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(response);
                }
            } catch (Exception exception) {
                byte[] response = "{\"ready\":false,\"error\":\"health_handler_failed\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, response.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(response);
                }
                LOGGER.error("Health endpoint failed", exception);
            }
        });
        server.createContext("/", exchange -> {
            byte[] response = "feature-pipeline-ready".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(response);
            }
        });
        server.start();
        LOGGER.info("Health server started on port {}", port);
    }

    private static void startMetricsPublisher(String bootstrapServers, String metricsTopic) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(metricsProducerProperties(bootstrapServers))) {
                long watermark = METRICS.currentWatermarkMillis.get();
                long lag = watermark > 0 ? Math.max(0L, System.currentTimeMillis() - watermark) : 0L;
                METRICS.watermarkLagMillis.set(lag);
                Map<String, Object> payload = new HashMap<>();
                payload.put("metric_name", "pipeline_health");
                payload.put("late_events_dropped", METRICS.lateEventsDropped.get());
                payload.put("consumed_events", METRICS.consumedEvents.get());
                payload.put("produced_features", METRICS.producedFeatures.get());
                payload.put("parse_errors", METRICS.parseErrors.get());
                payload.put("watermark_lag_ms", METRICS.watermarkLagMillis.get());
                payload.put("current_watermark", watermark > 0 ? ISO_FORMATTER.format(Instant.ofEpochMilli(watermark)) : null);
                payload.put("computed_at", ISO_FORMATTER.format(Instant.now()));
                producer.send(new ProducerRecord<>(metricsTopic, "pipeline-health", MAPPER.writeValueAsString(payload)));
                producer.flush();
            } catch (Exception exception) {
                LOGGER.warn("Unable to publish pipeline metrics to topic {}: {}", metricsTopic, exception.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private static Properties metricsProducerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("acks", "all");
        properties.put("retries", "10");
        properties.put("retry.backoff.ms", "1000");
        properties.put("request.timeout.ms", "30000");
        return properties;
    }

    private static UserEvent parseUserEventSafe(String json) {
        try {
            return MAPPER.readValue(json, UserEvent.class);
        } catch (IOException exception) {
            METRICS.incrementParseErrors();
            LOGGER.error("Unable to parse user event JSON: {}", json, exception);
            return null;
        }
    }

    private static ContentMetadata parseContentMetadataSafe(String json) {
        try {
            return MAPPER.readValue(json, ContentMetadata.class);
        } catch (IOException exception) {
            METRICS.incrementParseErrors();
            LOGGER.error("Unable to parse metadata JSON: {}", json, exception);
            return null;
        }
    }

    private static void waitForKafkaAndEnsureTopics(String bootstrapServers, Map<String, TopicSpec> requiredTopics, int maxAttempts, int retrySeconds) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.RETRIES_CONFIG, "10");
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1000");

        int attempts = 0;
        while (attempts < maxAttempts) {
            attempts++;
            try (AdminClient adminClient = AdminClient.create(props)) {
                adminClient.describeCluster().nodes().get(30, TimeUnit.SECONDS);
                ensureTopics(adminClient, requiredTopics);
                LOGGER.info("Kafka startup validation succeeded on attempt {}", attempts);
                return;
            } catch (Exception exception) {
                LOGGER.warn("Kafka not ready on attempt {}/{}: {}", attempts, maxAttempts, exception.getMessage());
                if (attempts >= maxAttempts) {
                    throw new RuntimeException("Kafka did not become ready after " + maxAttempts + " attempts", exception);
                }
                try {
                    TimeUnit.SECONDS.sleep(retrySeconds);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for Kafka startup", interruptedException);
                }
            }
        }
    }

    private static void ensureTopics(AdminClient adminClient, Map<String, TopicSpec> requiredTopics) throws Exception {
        var existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);
        List<NewTopic> missingTopics = requiredTopics.entrySet().stream()
                .filter(entry -> !existingTopics.contains(entry.getKey()))
                .map(entry -> {
                    TopicSpec spec = entry.getValue();
                    NewTopic topic = new NewTopic(entry.getKey(), spec.partitions(), spec.replicationFactor());
                    if (!spec.configs().isEmpty()) {
                        topic.configs(spec.configs());
                    }
                    return topic;
                })
                .toList();

        if (missingTopics.isEmpty()) {
            LOGGER.info("All required topics already exist: {}", requiredTopics.keySet());
            return;
        }

        CreateTopicsResult result = adminClient.createTopics(missingTopics);
        try {
            result.all().get(30, TimeUnit.SECONDS);
            LOGGER.info("Created missing topics: {}", missingTopics.stream().map(NewTopic::name).toList());
        } catch (Exception exception) {
            if (!(exception.getCause() instanceof TopicExistsException)) {
                throw exception;
            }
            LOGGER.info("Topic creation raced with another producer; continuing.");
        }
    }

    private static String env(String key, String defaultValue) {
        String propertyValue = System.getProperty(key);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }
        String envValue = System.getenv(key.toUpperCase().replace('.', '_'));
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return defaultValue;
    }

    public record UserEvent(String userId, String contentId, String eventType, long dwellTimeMs, String timestamp) {
        public long eventTimeMillis() {
            return Instant.parse(timestamp).toEpochMilli();
        }
    }

    public record ContentMetadata(String contentId, String category, String creatorId, String publishTimestamp) {}

    public record EnrichedEvent(String userId, String contentId, String eventType, long dwellTimeMs, String timestamp, String category, String creatorId) {}

    public record FeatureRecord(String entityId, String featureName, Object featureValue, String computedAt) {
        public String featureKey() {
            return entityId + ":" + featureName;
        }

        public String toJson() {
            try {
                return MAPPER.writeValueAsString(this);
            } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    public static final class PipelineMetrics {
        private final AtomicLong maxObservedEventTimeMillis = new AtomicLong(0L);
        private final AtomicLong currentWatermarkMillis = new AtomicLong(0L);
        private final AtomicLong watermarkLagMillis = new AtomicLong(0L);
        private final AtomicLong lateEventsDropped = new AtomicLong(0L);
        private final AtomicLong consumedEvents = new AtomicLong(0L);
        private final AtomicLong producedFeatures = new AtomicLong(0L);
        private final AtomicLong parseErrors = new AtomicLong(0L);

        void observeEventTime(long eventTimeMillis) {
            long maxEventTime = maxObservedEventTimeMillis.accumulateAndGet(eventTimeMillis, Math::max);
            currentWatermarkMillis.set(Math.max(0L, maxEventTime - Duration.ofSeconds(30).toMillis()));
        }

        void incrementLateEventsDropped() {
            lateEventsDropped.incrementAndGet();
        }

        void incrementConsumedEvents() {
            consumedEvents.incrementAndGet();
        }

        void incrementProducedFeatures() {
            producedFeatures.incrementAndGet();
        }

        void incrementParseErrors() {
            parseErrors.incrementAndGet();
        }
    }

    public record TopicSpec(int partitions, short replicationFactor, Map<String, String> configs) {}

    public static final class MetadataEnrichmentFunction extends BroadcastProcessFunction<UserEvent, ContentMetadata, EnrichedEvent> {
        private final MapStateDescriptor<String, ContentMetadata> descriptor;

        public MetadataEnrichmentFunction(MapStateDescriptor<String, ContentMetadata> descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public void processElement(UserEvent userEvent, BroadcastProcessFunction<UserEvent, ContentMetadata, EnrichedEvent>.ReadOnlyContext context, Collector<EnrichedEvent> collector) throws Exception {
            ContentMetadata metadata = context.getBroadcastState(descriptor).get(userEvent.contentId());
            String category = metadata == null ? "unknown" : metadata.category();
            String creatorId = metadata == null ? "unknown" : metadata.creatorId();
            collector.collect(new EnrichedEvent(userEvent.userId(), userEvent.contentId(), userEvent.eventType(), userEvent.dwellTimeMs(), userEvent.timestamp(), category, creatorId));
        }

        @Override
        public void processBroadcastElement(ContentMetadata contentMetadata, BroadcastProcessFunction<UserEvent, ContentMetadata, EnrichedEvent>.Context context, Collector<EnrichedEvent> collector) throws Exception {
            context.getBroadcastState(descriptor).put(contentMetadata.contentId(), contentMetadata);
        }
    }

    public static final class UserFeatureAggregate implements AggregateFunction<UserEvent, UserAccumulator, UserAccumulator> {
        @Override
        public UserAccumulator createAccumulator() {
            return new UserAccumulator();
        }

        @Override
        public UserAccumulator add(UserEvent value, UserAccumulator accumulator) {
            accumulator.totalEvents++;
            if ("click".equals(value.eventType())) {
                accumulator.clickEvents++;
            }
            accumulator.dwellSum += value.dwellTimeMs();
            return accumulator;
        }

        @Override
        public UserAccumulator getResult(UserAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public UserAccumulator merge(UserAccumulator a, UserAccumulator b) {
            UserAccumulator merged = new UserAccumulator();
            merged.totalEvents = a.totalEvents + b.totalEvents;
            merged.clickEvents = a.clickEvents + b.clickEvents;
            merged.dwellSum = a.dwellSum + b.dwellSum;
            return merged;
        }
    }

    public static final class UserFeatureWindowFunction extends ProcessWindowFunction<UserAccumulator, FeatureRecord, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<UserAccumulator> elements, Collector<FeatureRecord> collector) {
            UserAccumulator accumulator = elements.iterator().next();
            double clickRate = accumulator.totalEvents == 0 ? 0.0 : (double) accumulator.clickEvents / accumulator.totalEvents;
            double avgDwellTime = accumulator.totalEvents == 0 ? 0.0 : (double) accumulator.dwellSum / accumulator.totalEvents;
            String computedAt = ISO_FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));
            collector.collect(new FeatureRecord(userId, "click_rate", clickRate, computedAt));
            collector.collect(new FeatureRecord(userId, "avg_dwell_time", avgDwellTime, computedAt));
        }
    }

    public static final class ContentFeatureAggregate implements AggregateFunction<UserEvent, ContentAccumulator, ContentAccumulator> {
        @Override
        public ContentAccumulator createAccumulator() {
            return new ContentAccumulator();
        }

        @Override
        public ContentAccumulator add(UserEvent value, ContentAccumulator accumulator) {
            switch (value.eventType()) {
                case "view" -> accumulator.viewCount++;
                case "like" -> accumulator.likeCount++;
                case "share" -> accumulator.shareCount++;
                default -> {
                }
            }
            return accumulator;
        }

        @Override
        public ContentAccumulator getResult(ContentAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public ContentAccumulator merge(ContentAccumulator a, ContentAccumulator b) {
            ContentAccumulator merged = new ContentAccumulator();
            merged.viewCount = a.viewCount + b.viewCount;
            merged.likeCount = a.likeCount + b.likeCount;
            merged.shareCount = a.shareCount + b.shareCount;
            return merged;
        }
    }

    public static final class ContentFeatureWindowFunction extends ProcessWindowFunction<ContentAccumulator, FeatureRecord, String, TimeWindow> {
        @Override
        public void process(String contentId, Context context, Iterable<ContentAccumulator> elements, Collector<FeatureRecord> collector) {
            ContentAccumulator accumulator = elements.iterator().next();
            double engagementRate = accumulator.viewCount == 0 ? 0.0 : (double) (accumulator.likeCount + accumulator.shareCount) / accumulator.viewCount;
            String computedAt = ISO_FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));
            collector.collect(new FeatureRecord(contentId, "engagement_rate", engagementRate, computedAt));
        }
    }

    public static final class CategoryAccumulator {
        final Map<String, Long> counts = new HashMap<>();
    }

    public static final class CategoryAffinityAggregate implements AggregateFunction<EnrichedEvent, CategoryAccumulator, CategoryAccumulator> {
        @Override
        public CategoryAccumulator createAccumulator() {
            return new CategoryAccumulator();
        }

        @Override
        public CategoryAccumulator add(EnrichedEvent value, CategoryAccumulator accumulator) {
            String category = value.category() == null ? "unknown" : value.category();
            accumulator.counts.put(category, accumulator.counts.getOrDefault(category, 0L) + 1L);
            return accumulator;
        }

        @Override
        public CategoryAccumulator getResult(CategoryAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public CategoryAccumulator merge(CategoryAccumulator a, CategoryAccumulator b) {
            CategoryAccumulator merged = new CategoryAccumulator();
            merged.counts.putAll(a.counts);
            b.counts.forEach((key, value) -> merged.counts.put(key, merged.counts.getOrDefault(key, 0L) + value));
            return merged;
        }
    }

    public static final class CategoryAffinityWindowFunction extends ProcessWindowFunction<CategoryAccumulator, FeatureRecord, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<CategoryAccumulator> elements, Collector<FeatureRecord> collector) {
            CategoryAccumulator accumulator = elements.iterator().next();
            String computedAt = ISO_FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));
            accumulator.counts.forEach((category, count) -> collector.collect(new FeatureRecord(userId, "category_affinity_score:" + category, count, computedAt)));
        }
    }

    public static final class UserAccumulator {
        long totalEvents;
        long clickEvents;
        long dwellSum;
    }

    public static final class ContentAccumulator {
        long viewCount;
        long likeCount;
        long shareCount;
    }

    public static final class FeatureRecordKeySerializer implements SerializationSchema<FeatureRecord> {
        @Override
        public byte[] serialize(FeatureRecord element) {
            return element.featureKey().getBytes(StandardCharsets.UTF_8);
        }
    }

    public static final class FeatureRecordValueSerializer implements SerializationSchema<FeatureRecord> {
        @Override
        public byte[] serialize(FeatureRecord element) {
            return element.toJson().getBytes(StandardCharsets.UTF_8);
        }
    }
}