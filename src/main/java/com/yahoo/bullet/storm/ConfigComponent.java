/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;


import backtype.storm.topology.base.BaseComponent;

import java.util.Objects;

public abstract class ConfigComponent extends BaseComponent {
    private static final long serialVersionUID = 7024045327676195931L;

    protected BulletStormConfig config;

    /**
     * Creates an instance of this class with the given non-null config.
     *
     * @param config The non-null {@link BulletStormConfig} which is the config for this component.
     */
    public ConfigComponent(BulletStormConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }
}
