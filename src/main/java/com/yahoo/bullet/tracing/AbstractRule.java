/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.tracing;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.parsing.Specification;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractRule<T, R> implements Rule<T, R> {
    protected String ruleString;
    protected int duration;
    protected Specification specification;
    @Getter
    protected long startTime;

    /**
     * Constructor that takes a String representation of the rule and a configuration to use.
     *
     * @param ruleString The rule as a string.
     * @param configuration The configuration to use.
     * @throws ParsingException if there was an issue.
     */
    public AbstractRule(String ruleString, Map configuration) throws JsonParseException, ParsingException {
        this.ruleString = ruleString;
        specification = Parser.parse(ruleString, configuration);
        Optional<List<Error>> errors = specification.validate();
        if (errors.isPresent()) {
            throw new ParsingException(errors.get());
        }
        duration = specification.getDuration();
        startTime = System.currentTimeMillis();
    }

    /**
     * Returns true iff the rule has expired.
     *
     * @return boolean denoting if rule has expired.
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > startTime + duration;
    }

    @Override
    public String toString() {
        return ruleString;
    }
}
