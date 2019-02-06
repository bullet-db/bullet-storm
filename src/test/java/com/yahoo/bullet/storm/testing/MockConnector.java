/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.connector.BulletConnector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MockConnector extends BulletConnector {
    private boolean throwOnInitialize = false;
    private boolean throwOnClose = false;
    private boolean throwOnRead = false;
    private List<Object> messages = Arrays.asList(Collections.singletonMap("foo", "bar"), Collections.singletonMap("foo", 5), null);

    /**
     * Creates a MockConnector.
     *
     * @param config Not used.
     */
    public MockConnector(BulletConfig config) {
        super(config);
    }

    @Override
    public void initialize() throws BulletDSLException {
        if (throwOnInitialize) {
            throw new BulletDSLException("mock exception");
        }
        throwOnInitialize = true;
    }

    @Override
    public List<Object> read() throws BulletDSLException {
        if (throwOnRead) {
            throw new BulletDSLException("mock exception");
        }
        throwOnRead = true;
        return messages;
    }

    @Override
    public void close() throws Exception {
        if (throwOnClose) {
            throw new Exception("mock exception");
        }
        throwOnClose = true;
    }
}
