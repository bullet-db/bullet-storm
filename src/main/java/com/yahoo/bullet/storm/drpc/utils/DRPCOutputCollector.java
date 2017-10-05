/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm.drpc.utils;

import lombok.Getter;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class can be used as a collector for spouts and bolts that do not actually emit. This is used to
 * control the DRPCSpout spout and ReturnResults bolt classes to not actually do the emit.
 *
 *  ReturnResults does not need to emit but can call ack or fail
 *  DRPCSpout only needs the emit.
 */
public class DRPCOutputCollector implements ISpoutOutputCollector, IOutputCollector {
    private List<List<Object>> tuples = new ArrayList<>();
    @Getter
    private boolean acked = false;
    @Getter
    private boolean failed = false;

    /**
     * Returns and resets the last series of tuples emitted to this collector. This method is not idempotent. It resets
     * this collector state if there were any tuples to emit. Further calls will not return the tuples. They return null.
     *
     * @return The {@link List} of tuples emitted since the last call to this method.
     */
    public List<List<Object>> reset() {
        if (!haveOutput()) {
            return null;
        }
        List<List<Object>> toReturn = tuples;
        // Reset the processing
        tuples.clear();
        acked = false;
        failed = false;
        return toReturn;
    }

    /**
     * Returns true if we added any output since the last {@link #reset()}
     *
     * @return A boolean denoting if we have output.
     */
    public boolean haveOutput() {
        return !tuples.isEmpty();
    }

    // Bolt methods (used by ReturnResults)

    @Override
    public void ack(Tuple input) {
        // Just store that the last tuple was acked.
        acked = true;
    }

    @Override
    public void fail(Tuple input) {
        // Just store that the last tuple was failed.
        failed = true;
    }

    // Spout methods (used by DRPCSpout)

    @Override
    public List<Integer> emit(String streamID, List<Object> tuple, Object messageID) {
        // Ignore the streamID. Stick the messageID in as well.
        tuples.add(new Values(tuple, messageID));
        // We don't care about the return value
        return null;
    }

    // No need to support these methods. They are currently unused by the DRPCSpout and ReturnResults

    @Override
    public void reportError(Throwable error) {
        throw new UnsupportedOperationException("IErrorReporter reportError is not supported");
    }

    @Override
    public void resetTimeout(Tuple input) {
        throw new UnsupportedOperationException("Bolt resetTimeout is not supported");
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        throw new UnsupportedOperationException("Bolt emit is not supported");
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        throw new UnsupportedOperationException("Bolt emitDirect is not supported");
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        throw new UnsupportedOperationException("Spout emitDirect is not supported");
    }

    @Override
    public long getPendingCount() {
        throw new UnsupportedOperationException("Spout getPendingCount is not supported");
    }
}
