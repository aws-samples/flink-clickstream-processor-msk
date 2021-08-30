package com.amazonaws.kaja.samples;

import com.amazonaws.kaja.samples.utils.AmazonElasticsearchSink;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samples.clickstream.avro.ClickEvent;
import java.util.Optional;
import java.util.*;


public class ClickstreamProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ClickstreamProcessor.class);
    private static final List<String> MANDATORY_PARAMETERS = Arrays.asList("BootstrapServers");
    private static transient Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").create();


    public static <T> String toJson(T objToConvert){

        return gson.toJson(objToConvert);
    }

    private static <T> String tupleToString(T tupleToConvert) {
         if (tupleToConvert instanceof Tuple){
             List<String> returnVal = new ArrayList<>();
             Integer arity = ((Tuple) tupleToConvert).getArity();
             for (Integer i = 0; i < arity; i++){
                 returnVal.add(((Tuple) tupleToConvert).getField(i).toString());
             }
             return  String.join(",", returnVal);
         }
        return null;
    }

    public static void main(String[] args) throws Exception {

        //Setting up the ExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting TimeCharacteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Map<String, Properties> applicationProperties;

        if (env instanceof LocalStreamEnvironment) {
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties(Objects.requireNonNull(ClickstreamProcessor.class.getClassLoader().getResource("KDAApplicationProperties.json")).getPath());
            //Setting parallelism in code. When running on my laptop I was getting out of file handles error, so reduced parallelism, but increase it on KDA
            env.setParallelism(1);
            //Setting the Checkpoint interval. The Default for KDA App is 60,000 (or 1 min).
            // Here I'm setting it to 5 secs in the code which will override the KDA app setting
            env.enableCheckpointing(20000L);

        } else {
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties();
        }

        if (applicationProperties == null) {
            LOG.error("Unable to load application properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

        if (flinkProperties == null) {
            LOG.error("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        if (! flinkProperties.keySet().containsAll(MANDATORY_PARAMETERS)) {
            LOG.error("Missing mandatory parameters. Expected '{}' but found '{}'. Exiting.",
                    String.join(", ", MANDATORY_PARAMETERS),
                    flinkProperties.keySet());

            return;
        }

        //Setting properties for Apache kafka (MSK)
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", flinkProperties.getProperty("BootstrapServers"));
        kafkaConfig.setProperty("group.id", flinkProperties.getProperty("GroupId", "flink-clickstream-processor"));
        //kafkaConfig.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        //kafkaConfig.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");

        //Setting properties for Glue Schema Registry
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION,flinkProperties.getProperty("Region", "us-east-1"));
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

        //Setting the source for Apache kafka (MSK) and assigning Timestamps and watermarks for Event Time
        DataStream<ClickEvent> clickEvents = env
                .addSource(
                        new FlinkKafkaConsumer011<>(
                                flinkProperties.getProperty("Topic", "ExampleTopic"),
                                GlueSchemaRegistryAvroDeserializationSchema.forSpecific(ClickEvent.class, configs),
                                kafkaConfig).setStartFromEarliest().assignTimestampsAndWatermarks(new ClickEventTimestampWatermarkGenerator()));



        //Using Session windows with a gap of 1 sec since the Clickstream data generated uses a random gap
        // between 50 and 550 msecs
        //Creating User sessions and calculating the total number of events per session, no. of events before a buy
        //and unique departments(products) visited
        DataStream<UserIdSessionEvent> userSessionsAggregates = clickEvents
                .keyBy(ClickEvent::getUserid)
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
                .aggregate(new UserAggregate(), new UserAggWindowFunction());

        //Filtering out the sessions without a buy, so we only have the sessions with a buy
        DataStream<UserIdSessionEvent> userSessionsAggregatesWithOrderCheckout = userSessionsAggregates
                .filter((FilterFunction<UserIdSessionEvent>) userIdSessionEvent -> userIdSessionEvent.getOrderCheckoutEventCount() != 0);
        //userSessionsAggregatesWithOrderCheckout.print();

        //Calculating overall number of user sessions in the last 10 seconds (tumbling window), sessions with a buy and
        //percent of sessions with a buy
        //using a processwindowfunction in addition to aggregate to optimize the aggregation
        // and include window metadata like window start and end times
        DataStream<UserIdAggEvent> clickEventsUserIdAggResult =  userSessionsAggregates
                .keyBy(UserIdSessionEvent::getEventKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UserSessionAggregates(), new UserSessionWindowFunction());

        //Calculating unique departments visited and number of user sessions visiting the department
        // in the last 10 seconds (tumbling window)
        //This changes the key to key on departments
        DataStream<DepartmentsAggEvent> departmentsAgg = userSessionsAggregates
                .flatMap(new DepartmentsFlatMap())
                .keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new DepartmentsAggReduceFunction(), new DepartmentsAggWindowFunction())
                ;

        //Example of a streaming file sink to sink in json format to the local filesystem. Useful for dev and test
        //But not enabled for Prod on a KDA Application
//        final StreamingFileSink<DepartmentsAggEvent> sinkDepartmentsAgg = StreamingFileSink
//                .forRowFormat(new Path("file:///tmp/DepartmentsAgg"), (Encoder<DepartmentsAggEvent>) (element, stream) -> {
//                    PrintStream out = new PrintStream(stream);
//                    out.println(toJson(element));
//                })
//                //.withBucketAssigner()
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build();

//        final StreamingFileSink<DepartmentsAggEvent> sinkDepartmentsAgg = StreamingFileSink
//                .forBulkFormat(new Path("file:///tmp/DepartmentsAgg"), ParquetAvroWriters.forReflectRecord(DepartmentsAggEvent.class))
//                .withBucketAssigner(new BucketAssigner<DepartmentsAggEvent, Object>() {
//                    @Override
//                    public Object getBucketId(DepartmentsAggEvent departmentsAggEvent, Context context) {
//                        return null;
//                    }
//
//                    @Override
//                    public SimpleVersionedSerializer<Object> getSerializer() {
//                        return null;
//                    }
//                })
//                .build();


        //departmentsAgg.addSink(sinkDepartmentsAgg);

//        final StreamingFileSink<UserIdSessionEvent> sinkClickEventsUserIdOrderCheckout = StreamingFileSink
//                .forRowFormat(new Path("file:///tmp/UserIdOrderCheckout"), (Encoder<UserIdSessionEvent>) (element, stream) -> {
//                    PrintStream out = new PrintStream(stream);
//                    out.println(toJson(element));
//                })
//                //.withBucketAssigner()
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build();

        //userSessionsAggregatesWithOrderCheckout.addSink(sinkClickEventsUserIdOrderCheckout);

//        final StreamingFileSink<UserIdAggEvent> sinkclickEventsUserIdAggResult = StreamingFileSink
//                .forRowFormat(new Path("file:///tmp/UserIdAggResult"), (Encoder<UserIdAggEvent>) (element, stream) -> {
//                    PrintStream out = new PrintStream(stream);
//                    out.println(toJson(element));
//                })
//                //.withBucketAssigner()
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build();

        //clickEventsUserIdAggResult.addSink(sinkclickEventsUserIdAggResult);
        //Keyed serialization schema to provide a key which with null partitioner will allow Kafka to use the key to
        //hash partition messages across the topic partitions
        FlinkKafkaProducer011<DepartmentsAggEvent> kafkaDepts = new FlinkKafkaProducer011<>(flinkProperties.getProperty("DepartmentsAgg_Topic", "Departments_Agg"),
                new KeyedSerializationSchema<DepartmentsAggEvent>() {
                    @Override
                    public byte[] serializeKey(DepartmentsAggEvent element) {
                        return toJson(element.getDepartmentName()).getBytes();
                    }

                    @Override
                    public byte[] serializeValue(DepartmentsAggEvent element) {
                        return toJson(element).getBytes();
                    }

                    @Override
                    public String getTargetTopic(DepartmentsAggEvent element) {
                        return null;
                    }
                }, kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<DepartmentsAggEvent>) null));

        kafkaDepts.setWriteTimestampToKafka(true);
        departmentsAgg.addSink(kafkaDepts);

        //Non keyed serialization schema which with null partitioner will allow Kafka to
        //round robin messages across the topic partitions
        FlinkKafkaProducer011<UserIdAggEvent> kafkaUserIdAggEvents = new FlinkKafkaProducer011<>(flinkProperties.getProperty("clickEventsUserIdAggResult_Topic", "ClickEvents_UserId_Agg_Result"),
                new SerializationSchema<UserIdAggEvent>() {
                    @Override
                    public byte[] serialize(UserIdAggEvent element) {
                        return toJson(element).getBytes();
                    }
                }, kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<UserIdAggEvent>) null));
        kafkaUserIdAggEvents.setWriteTimestampToKafka(true);
        clickEventsUserIdAggResult.addSink(kafkaUserIdAggEvents);

        //Non keyed serialization schema which with null partitioner will allow Kafka to
        //round robin messages across the topic partitions
        FlinkKafkaProducer011<UserIdSessionEvent> kafkaUserIdSessionEvent = new FlinkKafkaProducer011<>(flinkProperties.getProperty("userSessionsAggregatesWithOrderCheckout_Topic", "User_Sessions_Aggregates_With_Order_Checkout"),
                new SerializationSchema<UserIdSessionEvent>() {
                    @Override
                    public byte[] serialize(UserIdSessionEvent element) {
                        return toJson(element).getBytes();
                    }
                }, kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<UserIdSessionEvent>) null));
        kafkaUserIdSessionEvent.setWriteTimestampToKafka(true);
        userSessionsAggregatesWithOrderCheckout.addSink(kafkaUserIdSessionEvent);


        //Creating Amazon Elasticsearch sinks and sinking the streams to it
        if (flinkProperties.containsKey("ElasticsearchEndpoint")) {
            String region;
            final String elasticsearchEndpoint = flinkProperties.getProperty("ElasticsearchEndpoint");

            if (env instanceof LocalStreamEnvironment) {
                region = flinkProperties.getProperty("Region");
            } else {
                region = flinkProperties.getProperty("Region", Regions.getCurrentRegion().getName());
            }

            departmentsAgg.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint, region, "departments_count", "departments_count"));
            clickEventsUserIdAggResult.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint, region, "user_session_counts", "user_session_counts"));
            userSessionsAggregatesWithOrderCheckout.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint, region, "user_session_details", "user_session_details"));
        }
        LOG.info("Starting execution..");
        env.execute();

    }
}
