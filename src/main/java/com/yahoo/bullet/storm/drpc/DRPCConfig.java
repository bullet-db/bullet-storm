/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import com.yahoo.bullet.storm.BulletStormConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
public class DRPCConfig extends BulletStormConfig {
    private static final long serialVersionUID = -2767564998976326025L;

    public static final String PREFIX = "bullet.pubsub.storm.drpc.";

    // Settings

    /** The location of DRPC servers. **/
    public static final String DRPC_SERVERS = PREFIX + "servers";
    /** This is the name of the DRPC function used to register with the DRPC servers. **/
    public static final String DRPC_FUNCTION = PREFIX + "function";
    /** This is the HTTP protocol to use when submitting to the DRPC server. **/
    public static final String DRPC_HTTP_PROTOCOL = PREFIX + "http.protocol";
    /** This is the port that the QUERY_SUBMISSION end talks to. **/
    public static final String DRPC_HTTP_PORT = PREFIX + "http.port";
    /** The path that queries must be POSTed to. This generally is "drpc". **/
    public static final String DRPC_HTTP_PATH = PREFIX + "http.path";
    /** The timeout for HTTP connections to DRPC servers. **/

    public static final String DRPC_HTTP_CONNECT_TIMEOUT = PREFIX + "http.connect.timeout.ms";
    /** The number of retries for HTTP connections to DRPC servers. **/
    public static final String DRPC_HTTP_CONNECT_RETRY_LIMIT = PREFIX + "http.connect.retry.limit";

    /** The maximum pending queries read by a single subscriber in QUERY_PROCESSING before a commit is needed. **/
    public static final String DRPC_MAX_UNCOMMITED_MESSAGES = PREFIX + "max.uncommitted.messages";

    // Defaults

    public static final List<String> DEFAULT_DRPC_SERVERS = Collections.singletonList("127.0.0.1");
    public static final String DEFAULT_DRPC_FUNCTION = "bullet-query";
    public static final String DEFAULT_DRPC_HTTP_PROTOCOL = "http";
    public static final String DEFAULT_DRPC_HTTP_PORT = "3774";
    public static final String DEFAULT_DRPC_HTTP_PATH = "drpc";
    public static final int DEFAULT_DRPC_HTTP_CONNECT_TIMEOUT = 5000;
    public static final int DEFAULT_DRPC_HTTP_CONNECT_RETRY_LIMIT = 3;
    public static final int DEFAULT_DRPC_MAX_UNCOMMITED_MESSAGES = 50;

    // Validations

    private static final Validator VALIDATOR = BulletStormConfig.getValidator();
    static {
        VALIDATOR.define(DRPC_SERVERS)
                 .checkIf(Validator::isList)
                 .defaultTo(DEFAULT_DRPC_SERVERS);
        VALIDATOR.define(DRPC_FUNCTION)
                 .checkIf(Validator::isString)
                 .defaultTo(DEFAULT_DRPC_FUNCTION);
        VALIDATOR.define(DRPC_HTTP_PROTOCOL)
                 .checkIf(Validator::isString)
                 .checkIf(Validator.isIn("http", "https"))
                 .defaultTo(DEFAULT_DRPC_HTTP_PROTOCOL);
        VALIDATOR.define(DRPC_HTTP_PORT)
                 .checkIf(DRPCConfig::isStringPositiveInteger)
                 .defaultTo(DEFAULT_DRPC_HTTP_PORT)
                 .castTo(Objects::toString);
        VALIDATOR.define(DRPC_HTTP_PATH)
                 .checkIf(Validator::isString)
                 .defaultTo(DEFAULT_DRPC_HTTP_PATH);

        VALIDATOR.define(DRPC_HTTP_CONNECT_TIMEOUT)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_DRPC_HTTP_CONNECT_TIMEOUT)
                 .castTo(Validator::asInt);
        VALIDATOR.define(DRPC_HTTP_CONNECT_RETRY_LIMIT)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_DRPC_HTTP_CONNECT_RETRY_LIMIT)
                 .castTo(Validator::asInt);

        VALIDATOR.define(DRPC_MAX_UNCOMMITED_MESSAGES)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_DRPC_MAX_UNCOMMITED_MESSAGES)
                 .castTo(Validator::asInt);

        // This throws a RunTimeException if windowing is not disabled because we do not want to proceed.
        VALIDATOR.relate("Windowing is not disabled", BulletConfig.WINDOW_DISABLE, BulletConfig.WINDOW_DISABLE)
                 .checkIf((mustBeTrue, ignored) -> failIfNotTrue(mustBeTrue));
    }

    /**
     * Create a new DRPCConfig by reading in a file.
     *
     * @param file The file containing DRPC settings.
     */
    public DRPCConfig(String file) {
        // Load and merge with default bullet-storm settings. Storm defaults also contain the DRPC settings.
        this(new BulletStormConfig(file));
        VALIDATOR.validate(this);
    }

    /**
     * Creates a new DRPCConfig wrapping the given config.
     *
     * @param config The config to wrap.
     */
    public DRPCConfig(Config config) {
        super(config);
        VALIDATOR.validate(this);
    }

    @Override
    public DRPCConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }

    private static boolean isStringPositiveInteger(Object entry) {
        if (entry == null || !(entry instanceof Number || entry instanceof String)) {
            log.warn("{} should be a valid positive integer", entry);
            return false;
        }
        try {
            String asString = entry.toString();
            Integer asInt = Integer.valueOf(asString);
            return Validator.isPositiveInt(asInt);
        } catch (NumberFormatException e) {
            log.warn("{} should be a string that is a valid positive integer", entry);
            return false;
        }
    }

    private static boolean failIfNotTrue(Object mustBeTrue) {
        Boolean windowDisabled = (Boolean) mustBeTrue;
        if (!windowDisabled) {
            log.error("DRPC does not support windowing. You must set {} to true", BulletConfig.WINDOW_DISABLE);
            throw new RuntimeException("DRPC does not support windowing. Disable windows first.");
        }
        return true;
    }
}
