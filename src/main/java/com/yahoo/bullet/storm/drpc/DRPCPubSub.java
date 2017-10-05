/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.pubsub.PubSub.Context.QUERY_SUBMISSION;

@Slf4j
public class DRPCPubSub extends PubSub {
    // This is so that in QUERY_SUBMISSION, the same publisher and subscriber instances are handed out.
    private List<DRPCQueryResultPubscriber> commonPool = new ArrayList<>();
    private int maxUncommittedMessages;

    /**
     * Create a DRPCPubSub using a {@link BulletConfig}.
     *
     * @param config The BulletConfig containing settings to create a new DRPCPubSub.
     * @throws PubSubException if there were errors in creating an instance.
     */
    public DRPCPubSub(BulletConfig config) throws PubSubException {
        super(config);
        this.config = new DRPCConfig(config);
        maxUncommittedMessages = getRequiredConfig(Number.class, DRPCConfig.DRPC_MAX_UNCOMMITED_MESSAGES).intValue();
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        return context == QUERY_SUBMISSION ? getPubscriber() : new DRPCQuerySubscriber(config, maxUncommittedMessages);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        return context == QUERY_SUBMISSION ? getPubscriber() : new DRPCResultPublisher(config);
    }

    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        if (context == QUERY_SUBMISSION) {
            return getPubscribers(n).stream().map(p -> (Subscriber) p).collect(Collectors.toList());
        }
        return IntStream.range(0, n).mapToObj(i -> new DRPCQuerySubscriber(config, maxUncommittedMessages))
                                    .collect(Collectors.toList());

    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        if (context == QUERY_SUBMISSION) {
            return getPubscribers(n).stream().map(p -> (Publisher) p).collect(Collectors.toList());
        }
        return IntStream.range(0, n).mapToObj(i -> new DRPCResultPublisher(config)).collect(Collectors.toList());
    }

    private DRPCQueryResultPubscriber getPubscriber() {
        return getPubscribers(1).get(0);
    }

    private List<DRPCQueryResultPubscriber> getPubscribers(int n) {
        if (commonPool.isEmpty()) {
            createPubscribers(n);
        }
        int size = commonPool.size();
        if (size != n) {
            log.warn("DRPCPubSub in QUERY_SUBMISSION MUST have the same publishers and subscribers. You asked for {} " +
                     "publishers or subscribers but had already created {} publishers or subscribers. Giving you the " +
                     "{} instances that you had already created.", n, size, size);
        }
        return commonPool;
    }

    private void createPubscribers(int amount) {
        IntStream.range(0, amount).forEach(i -> commonPool.add(new DRPCQueryResultPubscriber(config)));
    }
}
