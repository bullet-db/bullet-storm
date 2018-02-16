/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.topology.IRichSpout;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

@Slf4j
@SuppressWarnings("unchecked")
public class ReflectionUtils {
    // The name of the register method in a custom metrics class.
    public static final String REGISTER_METHOD = "register";

    /**
     *
     * Creates and initializes an instance of {@link IRichSpout} using a constructor that takes a {@link List} of String
     * arguments or the default constructor if one is not available.
     *
     * @param className The name of the spout class to load.
     * @param args The arguments to pass to its constructor that matches this type if it has one.
     * @return A created instance of this spout class.
     * @throws Exception if there were issues creating or instantiating this spout.
     */
    public static IRichSpout getSpout(String className, List<String> args) throws Exception {
        Class<? extends IRichSpout> spout = (Class<? extends IRichSpout>) Class.forName(className);
        IRichSpout initialized;
        try {
            Constructor constructor = spout.getConstructor(List.class);
            log.info("Initializing spout using constructor {} with args {}", constructor.toGenericString(), args);
            initialized = (IRichSpout) constructor.newInstance(args);
        } catch (Exception e) {
            log.info("Could not find or initialize a constructor taking a List. Trying default constructor...", e);
            initialized = spout.newInstance();
        }
        log.info("Initialized spout class {}", className);
        return initialized;
    }

    /**
     * Creates an instance of a custom {@link IMetricsConsumer} to add for custom metrics logging for the topology. It
     * then calls a static method with the signature register({@link Config}, {@link BulletStormConfig}) to let that
     * class define any custom metrics and parallelisms etc using the {@link BulletStormConfig} into the Storm
     * {@link Config}.
     *
     * @param className The name of the custom {@link IMetricsConsumer}.
     * @param stormConfig The Storm configuration that would be used by the register method.
     * @param bulletConfig The Bullet configuration to pass to the register method.
     */
    public static void registerMetricsConsumer(String className, Config stormConfig, BulletStormConfig bulletConfig) {
        try {
            Class<? extends IMetricsConsumer> consumer = (Class<? extends IMetricsConsumer>) Class.forName(className);
            Method method = consumer.getMethod(REGISTER_METHOD, Config.class, BulletStormConfig.class);
            log.info("Calling the IMetricsConsumer register method for class {} using method {}", className, method.toGenericString());
            method.invoke(null, stormConfig, bulletConfig);
            log.info("Registered the IMetricsConsumer class {}", className);
        } catch (Exception e) {
            log.info("Could not call the register method for " + className, e);
        }
    }

    /**
     * Checks to see if the given class name is an instance of a custom {@link IMetricsConsumer} that can be used in
     * the topology. That class must define a static method with the signature
     * register({@link Config}, {@link BulletStormConfig}) to let that class define any custom metrics and
     * parallelisms etc using the {@link BulletStormConfig} into the Storm {@link Config}.
     *
     * @param className The name of the custom {@link IMetricsConsumer}.
     * @return A boolean denoting whether the class represented by the name is a valid custom metrics consumer.
     */
    public static boolean isIMetricsConsumer(String className) {
        try {
            Class<? extends IMetricsConsumer> consumer = (Class<? extends IMetricsConsumer>) Class.forName(className);
            consumer.getMethod(REGISTER_METHOD, Config.class, BulletStormConfig.class);
        } catch (Exception e) {
            log.warn("The given class: {} was not a proper IMetricsConsumer with a {} method", className, REGISTER_METHOD);
            log.warn("Exception: {}", e);
            return false;
        }
        return true;
    }
}
