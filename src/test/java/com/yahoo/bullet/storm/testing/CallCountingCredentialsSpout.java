/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.testing;

import com.yahoo.bullet.common.BulletConfig;
import org.apache.storm.ICredentialsListener;

import java.util.Map;

public class CallCountingCredentialsSpout extends CallCountingSpout implements ICredentialsListener {
    private static final long serialVersionUID = 6959020155541556851L;

    public CallCountingCredentialsSpout(BulletConfig config) {
        super(config);
    }

    @Override
    public void setCredentials(Map<String, String> map) {
        // Explicitly adding this override to show why we have this spout - to call the non-interface parent method
        super.setCredentials(map);
    }
}
