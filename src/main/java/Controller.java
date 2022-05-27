import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Controller implements Runnable {
    


    private static final Logger log = LogManager.getLogger(Controller.class);

    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;


    static Long sleep;
    static double doublesleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;

    static Instant lastScaleTime;
    static long joiningTime;



    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    //////////////////////////////////////////////////////////////////////////////





    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();


    static double dynamicTotalMaxConsumptionRate = 0.0;
    static double dynamicAverageMaxConsumptionRate = 0.0;

    static double wsla = 5.0;

    static List<Consumer> assignment;

    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static Instant lastCGQuery;




    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr = admin.describeTopics(Collections.singletonList(topic));
        td = tdr.values().get(topic).get();

        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();
        lastCGQuery = Instant.now();



        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size());
    }


    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Controller.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();

        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

      /*  dynamicTotalMaxConsumptionRate = 0.0;
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members()) {
            log.info("Calling the consumer {} for its consumption rate ", memberDescription.host());

            float rate = callForConsumptionRate(memberDescription.host());
            dynamicTotalMaxConsumptionRate += rate;
        }

        dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate /
                (float) consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();

        log.info("The total consumption rate of the CG is {}", String.format("%.2f",dynamicTotalMaxConsumptionRate));
        log.info("The average consumption rate of the CG is {}", String.format("%.2f", dynamicAverageMaxConsumptionRate));*/

    }


    private static float callForConsumptionRate(String host) {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host.substring(1), 5002)
                .usePlaintext()
                .build();
        RateServiceGrpc.RateServiceBlockingStub rateServiceBlockingStub
                = RateServiceGrpc.newBlockingStub(managedChannel);
        RateRequest rateRequest = RateRequest.newBuilder().setRate("Give me your rate")
                .build();
        log.info("connected to server {}", host);
        RateResponse rateResponse = rateServiceBlockingStub.consumptionRate(rateRequest);
        log.info("Received response on the rate: " + String.format("%.2f",rateResponse.getRate()));
        managedChannel.shutdown();
        return rateResponse.getRate();
    }

    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();


        // log.info("Date(System.currentTimeMillis()) {}",new Date(System.currentTimeMillis()));
        for (TopicPartitionInfo p : td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
            requestTimestampOffsets2.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : td.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }

        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition t = new TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();


            partitions.get(p.partition()).setLag(latestOffset - committedoffset);

            if (timeoffset2 == -1) {
                timeoffset2 = latestOffset;
            }
            if (timeoffset1 == -1) {
                // NOT very critical condition
                currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
               // log.info("Arrival rate into partition {} is {}", t.partition(), currentPartitionArrivalRate);
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                log.info("Arrival rate into partition {} is {}", t.partition(), partitions.get(p.partition()).getArrivalRate());

                log.info("lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(p);


            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                //log.info(" timeoffset1 {}, timeoffset2 {}", timeoffset1, timeoffset2);
               // log.info("Arrival rate into partition {} is {}", t.partition(), currentPartitionArrivalRate);
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                log.info(" Arrival rate into partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getArrivalRate());

                log.info(" lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));


            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;

        }
        log.info("totalArrivalRate {}", totalArrivalRate);


        if (Duration.between(lastCGQuery, Instant.now()).toSeconds() >= 15) {
            queryConsumerGroup();
            lastCGQuery = Instant.now();
        }

        youMightWanttoScaleUsingBinPack();


    }








    private static void youMightWanttoScaleUsingBinPack() {

        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        //dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate / (double) (size);
        //binPackAndScale();

        if(Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 60) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }


    public static void scaleAsPerBinPack(int currentsize) {


        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //checking for scale up coooldown
            //TODO externalize these cool down
            if (Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() <= 60 /*2*doublesleep*/) {
                log.info("Scale up cooldown period has not elapsed yet not taking decisions");
                return;
            } else {
                log.info("We have to upscale by {}", replicasForscale);
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    log.info("I have Upscaled you should have {}", neededsize);

                }
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaleTime = Instant.now();



        } else {

            if (Duration.between(lastScaleDownDecision, Instant.now()).toSeconds() <= 60/* 4*doublesleep*/) {
                log.info("Scale down cooldown period has not elapsed yet not taking scale down decisions");
                return;
            } else {

                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    log.info("I have Downscaled you should have {}", neededsize);

                    lastScaleUpDecision = Instant.now();
                    lastScaleDownDecision = Instant.now();
                    lastCGQuery = Instant.now();
                    lastScaleTime = Instant.now();
                }
            }
        }
    }


    private static int binPackAndScale() {

        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>();

        for (Partition partition : partitions) {
            parts.add(new Partition(partition.getId(), partition.getLag(), partition.getArrivalRate()));
        }
        dynamicAverageMaxConsumptionRate = 95.0;

        long maxLagCapacity;

        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);

        consumers.add(new Consumer(consumerCount, maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }

        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its lag", partition.getId(),  String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", partition.getArrivalRate()));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }

        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {

                //TODO externalize these choices on the inout to the FFD bin pack
                   if (cons.getRemainingLagCapacity() >=   partition.getAverageLag() &&
                            cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    /////
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer(consumerCount, (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }

        log.info(" The BP scaler recommended {}", consumers.size());

        for (Consumer cons : consumers) {
            log.info("consumer {} is assigned the following partitions", cons.getId() );
            for(Partition p : cons.getAssignedPartitions()) {
                log.info("Partition {}", p.getId());
            }
        }
        assignment = consumers;
        return consumers.size();

    }


    @Override
    public void run() {
        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        doublesleep = (double) sleep / 1000.0;

        try {

            //Initial delay so that the producer has started.
            Thread.sleep(60*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        while (true) {
            log.info("New Iteration:");
            try {
                getCommittedLatestOffsetsAndLag();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Sleeping for {} seconds", sleep / 1000.0);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("End Iteration;");
            log.info("=============================================");
        }
    }







    //////////////////////////////////////////////////////////////////////////////
}




